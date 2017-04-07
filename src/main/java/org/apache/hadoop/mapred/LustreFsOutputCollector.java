// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.mapred;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LustreFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Task.CombineOutputCollector;
import org.apache.hadoop.mapred.Task.CombinerRunner;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;



@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
class LustreFsOutputCollector<K extends Object, V extends Object>
    implements MapOutputCollector<K, V>, IndexedSortable {
  
  private static final Log LOG = LogFactory.getLog(LustreFsOutputCollector.class);
  
  private int partitions;
  private JobConf job;
  private TaskReporter reporter;
  private Class<K> keyClass;
  private Class<V> valClass;
  private RawComparator<K> comparator;
  private Serializer<K> keySerializer;
  private Serializer<V> valSerializer;

  // k/v accounting
  private IntBuffer kvmeta; // metadata overlay on backing store
  int kvstart;            // marks origin of spill metadata
  int kvend;              // marks end of spill metadata
  int kvindex;            // marks end of fully serialized records

  int equator;            // marks origin of meta/serialization
  int bufstart;           // marks beginning of spill
  int bufend;             // marks beginning of collectable
  int bufmark;            // marks end of record
  int bufindex;           // marks end of collected
  int bufvoid;            // marks the point where we should stop
                          // reading at the end of the buffer

  byte[] kvbuffer;        // main output buffer
  private final byte[] b0 = new byte[0];

  private static final int VALSTART = 0;         // val offset in acct
  private static final int KEYSTART = 1;         // key offset in acct
  private static final int PARTITION = 2;        // partition offset in acct
  private static final int VALLEN = 3;           // length of value
  private static final int NMETA = 4;            // num meta ints
  private static final int METASIZE = NMETA * 4; // size in bytes

  // spill accounting
  private int maxRec;
  private int softLimit;
  boolean spillInProgress;;
  int bufferRemaining;
  volatile Throwable sortSpillException = null;

  private IndexedSorter sorter;
  final ReentrantLock spillLock = new ReentrantLock();
  final Condition spillDone = spillLock.newCondition();
  final Condition spillReady = spillLock.newCondition();
  final BlockingBuffer bb = new BlockingBuffer();
  volatile boolean spillThreadRunning = false;
  final SpillThread spillThread = new SpillThread();

  // Counters
  private Counters.Counter mapOutputByteCounter;
  private Counters.Counter mapOutputRecordCounter;
  private Counters.Counter fileOutputByteCounter;

  final ArrayList<SpillRecord> indexCacheList =
    new ArrayList<SpillRecord>();

  private MapTask mapTask;
  private MapOutputFile mapOutputFile;
  private Progress sortPhase;
  private Counters.Counter spilledRecordsCounter;

  private CombinerRunner<K, V> combinerRunner = null;
  private CombineOutputCollector<K, V> combineCollector;
  private CompressionCodec codec;
  private LustreFileSystem lustrefs;
  private int[] spillIndices;

  public LustreFsOutputCollector() {
  }

  @SuppressWarnings("unchecked")
  public void init(MapOutputCollector.Context context
                  ) throws IOException, ClassNotFoundException {
    job = context.getJobConf();
    reporter = context.getReporter();
    mapTask = context.getMapTask();
    mapOutputFile = mapTask.getMapOutputFile();
    sortPhase = mapTask.getSortPhase();
    spilledRecordsCounter = reporter.getCounter(TaskCounter.SPILLED_RECORDS);
    partitions = job.getNumReduceTasks();   
    lustrefs = (LustreFileSystem)FileSystem.get(LustreFileSystem.NAME, job);
    spillIndices = new int[partitions];
    for (int i = 0; i < partitions; i++) {
      spillIndices[i] = 0;
    }

    //sanity checks
    final float spillper =
      job.getFloat(JobContext.MAP_SORT_SPILL_PERCENT, (float)0.8);
    final int sortmb = job.getInt(JobContext.IO_SORT_MB, 100);
    if (spillper > (float)1.0 || spillper <= (float)0.0) {
      throw new IOException("Invalid \"" + JobContext.MAP_SORT_SPILL_PERCENT +
          "\": " + spillper);
    }
    if ((sortmb & 0x7FF) != sortmb) {
      throw new IOException(
          "Invalid \"" + JobContext.IO_SORT_MB + "\": " + sortmb);
    }
    sorter = ReflectionUtils.newInstance(job.getClass("map.sort.class",
          QuickSort.class, IndexedSorter.class), job);
    // buffers and accounting
    int maxMemUsage = sortmb << 20;
    maxMemUsage -= maxMemUsage % METASIZE;
    kvbuffer = new byte[maxMemUsage];
    bufvoid = kvbuffer.length;
    kvmeta = ByteBuffer.wrap(kvbuffer)
       .order(ByteOrder.nativeOrder())
       .asIntBuffer();
    setEquator(0);
    bufstart = bufend = bufindex = equator;
    kvstart = kvend = kvindex;

    maxRec = kvmeta.capacity() / NMETA;
    softLimit = (int)(kvbuffer.length * spillper);
    bufferRemaining = softLimit;
    if (LOG.isInfoEnabled()) {
      LOG.info(JobContext.IO_SORT_MB + ": " + sortmb);
      LOG.info("soft limit at " + softLimit);
      LOG.info("bufstart = " + bufstart + "; bufvoid = " + bufvoid);
      LOG.info("kvstart = " + kvstart + "; length = " + maxRec);
    }

    // k/v serialization
    comparator = job.getOutputKeyComparator();
    keyClass = (Class<K>)job.getMapOutputKeyClass();
    valClass = (Class<V>)job.getMapOutputValueClass();
    SerializationFactory serializationFactory = new SerializationFactory(job);
    keySerializer = serializationFactory.getSerializer(keyClass);
    keySerializer.open(bb);
    valSerializer = serializationFactory.getSerializer(valClass);
    valSerializer.open(bb);

    // output counters
    mapOutputByteCounter = reporter.getCounter(TaskCounter.MAP_OUTPUT_BYTES);
    mapOutputRecordCounter =
      reporter.getCounter(TaskCounter.MAP_OUTPUT_RECORDS);
    fileOutputByteCounter = reporter
        .getCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES);

    
    // compression
    if (job.getCompressMapOutput()) {
      Class<? extends CompressionCodec> codecClass =
        job.getMapOutputCompressorClass(DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, job);
    }

    // combiner
    final Counters.Counter combineInputCounter =
      reporter.getCounter(TaskCounter.COMBINE_INPUT_RECORDS);
    combinerRunner = CombinerRunner.create(job, getTaskID(), 
          combineInputCounter, reporter, null);
    
    if (combinerRunner != null) {
      final Counters.Counter combineOutputCounter =
        reporter.getCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);
      combineCollector= new CombineOutputCollector<K,V>(combineOutputCounter, reporter, job);
    }
        
    spillInProgress = false;
    spillThread.setDaemon(true);
    spillThread.setName("SpillThread");
    spillLock.lock();
    try {
      spillThread.start();
      while (!spillThreadRunning) {
        spillDone.await();
      }
    } catch (InterruptedException e) {
      throw new IOException("Spill thread failed to initialize", e);
    } finally {
      spillLock.unlock();
    }
    if (sortSpillException != null) {
      throw new IOException("Spill thread failed to initialize",
          sortSpillException);
    }
  }

  /**
   * Serialize the key, value to intermediate storage.
   * When this method returns, kvindex must refer to sufficient unused
   * storage to store one METADATA.
   */
  public synchronized void collect(K key, V value, final int partition
                                   ) throws IOException {
    reporter.progress();
    if (key.getClass() != keyClass) {
      throw new IOException("Type mismatch in key from map: expected "
                            + keyClass.getName() + ", received "
                            + key.getClass().getName());
    }
    if (value.getClass() != valClass) {
      throw new IOException("Type mismatch in value from map: expected "
                            + valClass.getName() + ", received "
                            + value.getClass().getName());
    }
    if (partition < 0 || partition >= partitions) {
      throw new IOException("Illegal partition for " + key + " (" +
          partition + ")");
    }
    checkSpillException();
    bufferRemaining -= METASIZE;
    if (bufferRemaining <= 0) {
      // start spill if the thread is not running and the soft limit has been
      // reached
      spillLock.lock();
      try {
        do {
          if (!spillInProgress) {
            final int kvbidx = 4 * kvindex;
            final int kvbend = 4 * kvend;
            // serialized, unspilled bytes always lie between kvindex and
            // bufindex, crossing the equator. Note that any void space
            // created by a reset must be included in "used" bytes
            final int bUsed = distanceTo(kvbidx, bufindex);
            final boolean bufsoftlimit = bUsed >= softLimit;
            if ((kvbend + METASIZE) % kvbuffer.length !=
                equator - (equator % METASIZE)) {
              // spill finished, reclaim space
              resetSpill();
              bufferRemaining = Math.min(
                  distanceTo(bufindex, kvbidx) - 2 * METASIZE,
                  softLimit - bUsed) - METASIZE;
              continue;
            } else if (bufsoftlimit && kvindex != kvend) {
              // spill records, if any collected; check latter, as it may
              // be possible for metadata alignment to hit spill pcnt
              startSpill();
              final int avgRec = (int)
                (mapOutputByteCounter.getCounter() /
                mapOutputRecordCounter.getCounter());
              // leave at least half the split buffer for serialization data
              // ensure that kvindex >= bufindex
              final int distkvi = distanceTo(bufindex, kvbidx);
              final int newPos = (bufindex +
                Math.max(2 * METASIZE - 1,
                        Math.min(distkvi / 2,
                                 distkvi / (METASIZE + avgRec) * METASIZE)))
                % kvbuffer.length;
              setEquator(newPos);
              bufmark = bufindex = newPos;
              final int serBound = 4 * kvend;
              // bytes remaining before the lock must be held and limits
              // checked is the minimum of three arcs: the metadata space, the
              // serialization space, and the soft limit
              bufferRemaining = Math.min(
                  // metadata max
                  distanceTo(bufend, newPos),
                  Math.min(
                    // serialization max
                    distanceTo(newPos, serBound),
                    // soft limit
                    softLimit)) - 2 * METASIZE;
            }
          }
        } while (false);
      } finally {
        spillLock.unlock();
      }
    }

    try {
      // serialize key bytes into buffer
      int keystart = bufindex;
      keySerializer.serialize(key);
      if (bufindex < keystart) {
        // wrapped the key; must make contiguous
        bb.shiftBufferedKey();
        keystart = 0;
      }
      // serialize value bytes into buffer
      final int valstart = bufindex;
      valSerializer.serialize(value);
      // It's possible for records to have zero length, i.e. the serializer
      // will perform no writes. To ensure that the boundary conditions are
      // checked and that the kvindex invariant is maintained, perform a
      // zero-length write into the buffer. The logic monitoring this could be
      // moved into collect, but this is cleaner and inexpensive. For now, it
      // is acceptable.
      bb.write(b0, 0, 0);

      // the record must be marked after the preceding write, as the metadata
      // for this record are not yet written
      int valend = bb.markRecord();

      mapOutputRecordCounter.increment(1);
      mapOutputByteCounter.increment(
          distanceTo(keystart, valend, bufvoid));

      // write accounting info
      kvmeta.put(kvindex + PARTITION, partition);
      kvmeta.put(kvindex + KEYSTART, keystart);
      kvmeta.put(kvindex + VALSTART, valstart);
      kvmeta.put(kvindex + VALLEN, distanceTo(valstart, valend));
      // advance kvindex
      kvindex = (kvindex - NMETA + kvmeta.capacity()) % kvmeta.capacity();
    } catch (MapBufferTooSmallException e) {
      LOG.info("Record too large for in-memory buffer: " + e.getMessage());
      spillSingleRecord(key, value, partition);
      mapOutputRecordCounter.increment(1);
      return;
    }
  }

  private TaskAttemptID getTaskID() {
    return mapTask.getTaskID();
  }

  /**
   * Set the point from which meta and serialization data expand. The meta
   * indices are aligned with the buffer, so metadata never spans the ends of
   * the circular buffer.
   */
  private void setEquator(int pos) {
    equator = pos;
    // set index prior to first entry, aligned at meta boundary
    final int aligned = pos - (pos % METASIZE);
    kvindex =
      ((aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4;
    if (LOG.isInfoEnabled()) {
      LOG.info("(EQUATOR) " + pos + " kvi " + kvindex +
          "(" + (kvindex * 4) + ")");
    }
  }

  /**
   * The spill is complete, so set the buffer and meta indices to be equal to
   * the new equator to free space for continuing collection. Note that when
   * kvindex == kvend == kvstart, the buffer is empty.
   */
  private void resetSpill() {
    final int e = equator;
    bufstart = bufend = e;
    final int aligned = e - (e % METASIZE);
    // set start/end to point to first meta record
    kvstart = kvend =
      ((aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4;
    if (LOG.isInfoEnabled()) {
      LOG.info("(RESET) equator " + e + " kv " + kvstart + "(" +
        (kvstart * 4) + ")" + " kvi " + kvindex + "(" + (kvindex * 4) + ")");
    }
  }

  /**
   * Compute the distance in bytes between two indices in the serialization
   * buffer.
   * @see #distanceTo(int,int,int)
   */
  final int distanceTo(final int i, final int j) {
    return distanceTo(i, j, kvbuffer.length);
  }

  /**
   * Compute the distance between two indices in the circular buffer given the
   * max distance.
   */
  int distanceTo(final int i, final int j, final int mod) {
    return i <= j
      ? j - i
      : mod - i + j;
  }

  /**
   * For the given meta position, return the offset into the int-sized
   * kvmeta buffer.
   */
  int offsetFor(int metapos) {
    return metapos * NMETA;
  }

  /**
   * Compare logical range, st i, j MOD offset capacity.
   * Compare by partition, then by key.
   * @see IndexedSortable#compare
   */
  public int compare(final int mi, final int mj) {
    final int kvi = offsetFor(mi % maxRec);
    final int kvj = offsetFor(mj % maxRec);
    final int kvip = kvmeta.get(kvi + PARTITION);
    final int kvjp = kvmeta.get(kvj + PARTITION);
    // sort by partition
    if (kvip != kvjp) {
      return kvip - kvjp;
    }
    // sort by key
    return comparator.compare(kvbuffer,
        kvmeta.get(kvi + KEYSTART),
        kvmeta.get(kvi + VALSTART) - kvmeta.get(kvi + KEYSTART),
        kvbuffer,
        kvmeta.get(kvj + KEYSTART),
        kvmeta.get(kvj + VALSTART) - kvmeta.get(kvj + KEYSTART));
  }

  final byte META_BUFFER_TMP[] = new byte[METASIZE];
  
  /**
   * Swap metadata for items i, j
   * @see IndexedSortable#swap
   */
  public void swap(final int mi, final int mj) {
    int iOff = (mi % maxRec) * METASIZE;
    int jOff = (mj % maxRec) * METASIZE;
    System.arraycopy(kvbuffer, iOff, META_BUFFER_TMP, 0, METASIZE);
    System.arraycopy(kvbuffer, jOff, kvbuffer, iOff, METASIZE);
    System.arraycopy(META_BUFFER_TMP, 0, kvbuffer, jOff, METASIZE);
  }

  /**
   * Inner class managing the spill of serialized records to disk.
   */
  protected class BlockingBuffer extends DataOutputStream {

    public BlockingBuffer() {
      super(new Buffer());
    }

    /**
     * Mark end of record. Note that this is required if the buffer is to
     * cut the spill in the proper place.
     */
    public int markRecord() {
      bufmark = bufindex;
      return bufindex;
    }

    /**
     * Set position from last mark to end of writable buffer, then rewrite
     * the data between last mark and kvindex.
     * This handles a special case where the key wraps around the buffer.
     * If the key is to be passed to a RawComparator, then it must be
     * contiguous in the buffer. This recopies the data in the buffer back
     * into itself, but starting at the beginning of the buffer. Note that
     * this method should <b>only</b> be called immediately after detecting
     * this condition. To call it at any other time is undefined and would
     * likely result in data loss or corruption.
     * @see #markRecord()
     */
    protected void shiftBufferedKey() throws IOException {
      // spillLock unnecessary; both kvend and kvindex are current
      int headbytelen = bufvoid - bufmark;
      bufvoid = bufmark;
      final int kvbidx = 4 * kvindex;
      final int kvbend = 4 * kvend;
      final int avail =
        Math.min(distanceTo(0, kvbidx), distanceTo(0, kvbend));
      if (bufindex + headbytelen < avail) {
        System.arraycopy(kvbuffer, 0, kvbuffer, headbytelen, bufindex);
        System.arraycopy(kvbuffer, bufvoid, kvbuffer, 0, headbytelen);
        bufindex += headbytelen;
        bufferRemaining -= kvbuffer.length - bufvoid;
      } else {
        byte[] keytmp = new byte[bufindex];
        System.arraycopy(kvbuffer, 0, keytmp, 0, bufindex);
        bufindex = 0;
        out.write(kvbuffer, bufmark, headbytelen);
        out.write(keytmp);
      }
    }
  }

  public class Buffer extends OutputStream {
    private final byte[] scratch = new byte[1];

    @Override
    public void write(int v)
        throws IOException {
      scratch[0] = (byte)v;
      write(scratch, 0, 1);
    }

    /**
     * Attempt to write a sequence of bytes to the collection buffer.
     * This method will block if the spill thread is running and it
     * cannot write.
     * @throws MapBufferTooSmallException if record is too large to
     *    deserialize into the collection buffer.
     */
    @Override
    public void write(byte b[], int off, int len)
        throws IOException {
      // must always verify the invariant that at least METASIZE bytes are
      // available beyond kvindex, even when len == 0
      bufferRemaining -= len;
      if (bufferRemaining <= 0) {
        // writing these bytes could exhaust available buffer space or fill
        // the buffer to soft limit. check if spill or blocking are necessary
        boolean blockwrite = false;
        spillLock.lock();
        try {
          do {
            checkSpillException();

            final int kvbidx = 4 * kvindex;
            final int kvbend = 4 * kvend;
            // ser distance to key index
            final int distkvi = distanceTo(bufindex, kvbidx);
            // ser distance to spill end index
            final int distkve = distanceTo(bufindex, kvbend);

            // if kvindex is closer than kvend, then a spill is neither in
            // progress nor complete and reset since the lock was held. The
            // write should block only if there is insufficient space to
            // complete the current write, write the metadata for this record,
            // and write the metadata for the next record. If kvend is closer,
            // then the write should block if there is too little space for
            // either the metadata or the current write. Note that collect
            // ensures its metadata requirement with a zero-length write
            blockwrite = distkvi <= distkve
              ? distkvi <= len + 2 * METASIZE
              : distkve <= len || distanceTo(bufend, kvbidx) < 2 * METASIZE;

            if (!spillInProgress) {
              if (blockwrite) {
                if ((kvbend + METASIZE) % kvbuffer.length !=
                    equator - (equator % METASIZE)) {
                  // spill finished, reclaim space
                  // need to use meta exclusively; zero-len rec & 100% spill
                  // pcnt would fail
                  resetSpill(); // resetSpill doesn't move bufindex, kvindex
                  bufferRemaining = Math.min(
                      distkvi - 2 * METASIZE,
                      softLimit - distanceTo(kvbidx, bufindex)) - len;
                  continue;
                }
                // we have records we can spill; only spill if blocked
                if (kvindex != kvend) {
                  startSpill();
                  // Blocked on this write, waiting for the spill just
                  // initiated to finish. Instead of repositioning the marker
                  // and copying the partial record, we set the record start
                  // to be the new equator
                  setEquator(bufmark);
                } else {
                  // We have no buffered records, and this record is too large
                  // to write into kvbuffer. We must spill it directly from
                  // collect
                  final int size = distanceTo(bufstart, bufindex) + len;
                  setEquator(0);
                  bufstart = bufend = bufindex = equator;
                  kvstart = kvend = kvindex;
                  bufvoid = kvbuffer.length;
                  throw new MapBufferTooSmallException(size + " bytes");
                }
              }
            }

            if (blockwrite) {
              // wait for spill
              try {
                while (spillInProgress) {
                  reporter.progress();
                  spillDone.await();
                }
              } catch (InterruptedException e) {
                  throw new IOException(
                      "Buffer interrupted while waiting for the writer", e);
              }
            }
          } while (blockwrite);
        } finally {
          spillLock.unlock();
        }
      }
      // here, we know that we have sufficient space to write
      if (bufindex + len > bufvoid) {
        final int gaplen = bufvoid - bufindex;
        System.arraycopy(b, off, kvbuffer, bufindex, gaplen);
        len -= gaplen;
        off += gaplen;
        bufindex = 0;
      }
      System.arraycopy(b, off, kvbuffer, bufindex, len);
      bufindex += len;
    }
  }

  public void flush() throws IOException, ClassNotFoundException,
         InterruptedException {
    LOG.info("Starting flush of map output");
    spillLock.lock();
    try {
      while (spillInProgress) {
        reporter.progress();
        spillDone.await();
      }
      checkSpillException();

      final int kvbend = 4 * kvend;
      if ((kvbend + METASIZE) % kvbuffer.length !=
          equator - (equator % METASIZE)) {
        // spill finished
        resetSpill();
      }
      if (kvindex != kvend) {
        kvend = (kvindex + NMETA) % kvmeta.capacity();
        bufend = bufmark;
        if (LOG.isInfoEnabled()) {
          LOG.info("Spilling map output");
          LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
                   "; bufvoid = " + bufvoid);
          LOG.info("kvstart = " + kvstart + "(" + (kvstart * 4) +
                   "); kvend = " + kvend + "(" + (kvend * 4) +
                   "); length = " + (distanceTo(kvend, kvstart,
                         kvmeta.capacity()) + 1) + "/" + maxRec);
        }
        sortAndSpill();
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for the writer", e);
    } finally {
      spillLock.unlock();
    }
    assert !spillLock.isHeldByCurrentThread();
    // shut down spill thread and wait for it to exit. Since the preceding
    // ensures that it is finished with its work (and sortAndSpill did not
    // throw), we elect to use an interrupt instead of setting a flag.
    // Spilling simultaneously from this thread while the spill thread
    // finishes its work might be both a useful way to extend this and also
    // sufficient motivation for the latter approach.
    try {
      spillThread.interrupt();
      spillThread.join();
    } catch (InterruptedException e) {
      throw new IOException("Spill failed", e);
    }
    // release sort buffer before the merge
    kvbuffer = null;
    mergeParts();
  }

  public void close() { }

  protected class SpillThread extends Thread {

    @Override
    public void run() {
      spillLock.lock();
      spillThreadRunning = true;
      try {
        while (true) {
          spillDone.signal();
          while (!spillInProgress) {
            spillReady.await();
          }
          try {
            spillLock.unlock();
            sortAndSpill();
          } catch (Throwable t) {
            sortSpillException = t;
          } finally {
            spillLock.lock();
            if (bufend < bufstart) {
              bufvoid = kvbuffer.length;
            }
            kvstart = kvend;
            bufstart = bufend;
            spillInProgress = false;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        spillLock.unlock();
        spillThreadRunning = false;
      }
    }
  }

  private void checkSpillException() throws IOException {
    final Throwable lspillException = sortSpillException;
    if (lspillException != null) {
      if (lspillException instanceof Error) {
        final String logMsg = "Task " + getTaskID() + " failed : " +
          StringUtils.stringifyException(lspillException);
        mapTask.reportFatalError(getTaskID(), lspillException, logMsg);
      }
      throw new IOException("Spill failed", lspillException);
    }
  }

  private void startSpill() {
    assert !spillInProgress;
    kvend = (kvindex + NMETA) % kvmeta.capacity();
    bufend = bufmark;
    spillInProgress = true;
    if (LOG.isInfoEnabled()) {
      LOG.info("Spilling map output");
      LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
               "; bufvoid = " + bufvoid);
      LOG.info("kvstart = " + kvstart + "(" + (kvstart * 4) +
               "); kvend = " + kvend + "(" + (kvend * 4) +
               "); length = " + (distanceTo(kvend, kvstart,
                     kvmeta.capacity()) + 1) + "/" + maxRec);
    }
    spillReady.signal();
  }

  private void sortAndSpill() throws IOException, ClassNotFoundException,
                                     InterruptedException {    
      
    final int mstart = kvend / NMETA;
    final int mend = 1 + // kvend is a valid record
      (kvstart >= kvend
      ? kvstart
      : kvmeta.capacity() + kvstart) / NMETA;
    sorter.sort(LustreFsOutputCollector.this, mstart, mend, reporter);
    int spindex = mstart;
    for (int i = 0; i < partitions; ++i) {
      int spstart = spindex;
      while (spindex < mend &&
          kvmeta.get(offsetFor(spindex % maxRec)
                    + PARTITION) == i) {
        ++spindex;
      }
      if (spstart != spindex) {
        writePartition(new MRResultIterator(spstart, spindex), i);
      }                
    }
  }

  /**
   * Handles the degenerate case where serialization fails to fit in
   * the in-memory buffer, so we must spill the record from collect
   * directly to a spill file. Consider this "losing".
   */
  private void spillSingleRecord(final K key, final V value,
                                 int partition) throws IOException {
    for (int i = 0; i < partitions; ++i) {
      // create spill file
      IFile.Writer<K, V> writer = null;
      try {
        // Create a new codec, don't care!
        writer = getFileWriter(i);
        if (i == partition) {
          writer.append(key, value);
        }
        writer.close();
        mapOutputByteCounter.increment(writer.getRawLength());
        fileOutputByteCounter.increment(writer.getCompressedLength());
        writer = null;
      } catch (IOException e) {
        if (null != writer) writer.close();
        throw e;
      }
    }
  }
  
  public long writePartition(RawKeyValueIterator kvIter, int partition) throws IOException {
    Writer<K, V> writer = getFileWriter(partition);
    try {
      if (combinerRunner == null) {
        Merger.writeFile(kvIter, writer, reporter, job);
      } else {
        combineCollector.setWriter(writer);
        try {
          combinerRunner.combine(kvIter, combineCollector);
        } catch (Throwable t) {
          throw ((t instanceof IOException) ? (IOException) t : new IOException(t));
        }
      }
    } finally {
      writer.close();
      if (combineCollector != null) {
        combineCollector.setWriter(null);
      }
    }
    return writer.getCompressedLength();
  }

  public Writer<K, V> getFileWriter(int partition) throws IOException {
    int spillIndex, mapid = getTaskID().getTaskID().getId();
    spillIndex = spillIndices[partition]++;
    Path path = new Path(SharedFsPlugins.getTempPath(job, getTaskID().getJobID()), 
        String.format(SharedFsPlugins.MAP_OUTPUT, partition, mapid, spillIndex));
    return new Writer<K, V>(job, lustrefs.create(path), keyClass, valClass, codec, spilledRecordsCounter, true);
  }

  /**
   * Given an offset, populate vbytes with the associated set of
   * deserialized value bytes. Should only be called during a spill.
   */
  private void getVBytesForOffset(int kvoff, InMemValBytes vbytes) {
    // get the keystart for the next serialized value to be the end
    // of this value. If this is the last value in the buffer, use bufend
    final int vallen = kvmeta.get(kvoff + VALLEN);
    assert vallen >= 0;
    vbytes.reset(kvbuffer, kvmeta.get(kvoff + VALSTART), vallen);
  }

  /**
   * Inner class wrapping valuebytes, used for appendRaw.
   */
  protected class InMemValBytes extends DataInputBuffer {
    private byte[] buffer;
    private int start;
    private int length;

    public void reset(byte[] buffer, int start, int length) {
      this.buffer = buffer;
      this.start = start;
      this.length = length;

      if (start + length > bufvoid) {
        this.buffer = new byte[this.length];
        final int taillen = bufvoid - start;
        System.arraycopy(buffer, start, this.buffer, 0, taillen);
        System.arraycopy(buffer, 0, this.buffer, taillen, length-taillen);
        this.start = 0;
      }

      super.reset(this.buffer, this.start, this.length);
    }
  }

  protected class MRResultIterator implements RawKeyValueIterator {
    private final DataInputBuffer keybuf = new DataInputBuffer();
    private final InMemValBytes vbytes = new InMemValBytes();
    private final int end;
    private int current;
    public MRResultIterator(int start, int end) {
      this.end = end;
      current = start - 1;
    }
    public boolean next() throws IOException {
      return ++current < end;
    }
    public DataInputBuffer getKey() throws IOException {
      final int kvoff = offsetFor(current % maxRec);
      keybuf.reset(kvbuffer, kvmeta.get(kvoff + KEYSTART),
          kvmeta.get(kvoff + VALSTART) - kvmeta.get(kvoff + KEYSTART));
      return keybuf;
    }
    public DataInputBuffer getValue() throws IOException {
      getVBytesForOffset(offsetFor(current % maxRec), vbytes);
      return vbytes;
    }
    public Progress getProgress() {
      return null;
    }
    public void close() { }
  }

  protected void mergeParts() throws IOException, InterruptedException, ClassNotFoundException {

    // MR freaks out if it doesn't find file.out. Create a dummy file.
    FileSystem.getLocal(job).create(mapOutputFile.getOutputFileForWrite(0)).close();
  }
} // SharedFsOutputCollector


@SuppressWarnings("serial")
class MapBufferTooSmallException extends IOException {
  public MapBufferTooSmallException(String s) {
    super(s);
  }
}
