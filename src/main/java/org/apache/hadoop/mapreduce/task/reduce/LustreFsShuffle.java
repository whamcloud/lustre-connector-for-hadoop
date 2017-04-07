// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LustreFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.Merger;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SharedFsPlugins;
import org.apache.hadoop.mapred.ShuffleConsumerPlugin;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progress;

@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
@SuppressWarnings({"rawtypes"})
public class LustreFsShuffle<K, V> implements ShuffleConsumerPlugin<K, V>, ExceptionReporter {
    
    private static final Log LOG = LogFactory.getLog(LustreFsShuffle.class);
    
    protected static final int PROGRESS_FREQUENCY = 5;
    protected static final int MAX_EVENTS_TO_FETCH = 10000;
    protected static final int MIN_EVENTS_TO_FETCH = 100;
    protected static final int MAX_RPC_OUTSTANDING_EVENTS = 3000000;
    
    protected TaskAttemptID reduceId;
    protected JobConf jobConf;
    protected Reporter reporter;
    protected ShuffleClientMetrics metrics;
    protected TaskUmbilicalProtocol umbilical;
    
    protected ShuffleSchedulerImpl<K, V> scheduler;
    protected Throwable throwable = null;
    protected String throwingThreadName = null;
    protected Progress copyPhase;
    protected TaskStatus taskStatus;
    protected Task reduceTask; // Used for status updates
    
    protected Path mapOutputDir = null;
    protected Path reduceDir = null;
    protected Path mergeTempDir = null;
    protected LustreFileSystem lustrefs = null;
    
    Set<Segment<K, V>> segmentsToBeMerged = new TreeSet<Segment<K, V>>(
                                               new Comparator<Segment<K,V>>() {
                                                   
                                                   @Override
                                                   public int compare(Segment<K, V> seg1, Segment<K, V> seg2) {
                                                       if(seg1.getLength() < seg2.getLength()) {
                                                           return -1;
                                                       }
                                                       else if(seg1.getLength() > seg2.getLength()) {
                                                           return 1;
                                                       }
                                                       return SharedFsPlugins.getSegmentPath(seg1)
                                                       .compareTo(SharedFsPlugins.getSegmentPath(seg2));
                                                   }
                                               });
    private FileMerger merger;
    
    private int ioSortFactor;
    
    private Counters.Counter spilledRecordsCounter;
    private Counters.Counter mergedMapOutputsCounter;
    private CompressionCodec codec;
    private Progress mergePhase;
    
    
    @Override
    public void init(ShuffleConsumerPlugin.Context context) {
        
        this.reduceId = context.getReduceId();
        this.jobConf = context.getJobConf();
        this.umbilical = context.getUmbilical();
        this.reporter = context.getReporter();
        this.copyPhase = context.getCopyPhase();
        this.mergePhase = context.getMergePhase();
        this.taskStatus = context.getStatus();
        this.reduceTask = context.getReduceTask();
        this.codec = context.getCodec();
        this.spilledRecordsCounter = context.getSpilledRecordsCounter();
        this.mergedMapOutputsCounter = context.getMergedMapOutputsCounter();
        
        jobConf.setBoolean(MRConfig.MAPRED_IFILE_READAHEAD, false);
        try {
            lustrefs = (LustreFileSystem)FileSystem.get(LustreFileSystem.NAME, jobConf);
            mapOutputDir = SharedFsPlugins.getTempPath(jobConf,
                                                       JobID.downgrade(reduceId.getJobID()));
            reduceDir = new Path(mapOutputDir,
                                 String.format(SharedFsPlugins.MAP_OUTPUT,
                                               reduceId.getTaskID().getId(), 0, 0)).getParent();
            mergeTempDir = new Path(mapOutputDir, "temp");
        } catch (IOException ioe) {
            throw new RuntimeException("Map Output directory not found !!", ioe);
        }
        
        // Scheduler
        scheduler = new ShuffleSchedulerImpl<K, V>(
                                                   jobConf, taskStatus, reduceId, this, copyPhase,
                                                   context.getShuffledMapsCounter(),
                                                   context.getReduceShuffleBytes(),
                                                   context.getFailedShuffleCounter());
        
        this.ioSortFactor = jobConf.getInt(MRJobConfig.IO_SORT_FACTOR, 100);
        
        this.merger = new FileMerger();
        this.merger.start();
    }
    
    @Override
    public void close(){ }
    
    protected void mergeMapOutput(TaskAttemptID mapId) throws IOException {
        int mapid = mapId.getTaskID().getId();
        for(int i=0; true; i++) {
            Path file = new Path(mapOutputDir,
                                 String.format(SharedFsPlugins.MAP_OUTPUT,
                                               reduceId.getTaskID().getId(), mapid, i));
            if(!lustrefs.exists(file)) {
//                if(i == 0) {
//                    throw new IOException("No map outputs found. At least one is expected!");
//                }
                return;
            }
            addMapOutputSegments(file);
        }
    }
    
    private void addMapOutputSegments(Path file) throws IOException {
        addSegmentToMerge(new Segment<K, V>(jobConf, lustrefs, file, codec, false, mergedMapOutputsCounter));
    }
    
    @Override
    public RawKeyValueIterator run() throws IOException, InterruptedException {
        
        // Scale the maximum events we fetch per RPC call to mitigate OOM issues
        // on the ApplicationMaster when a thundering herd of reducers fetch events
        // TODO: This should not be necessary after HADOOP-8942
        int eventsPerReducer = Math.max(MIN_EVENTS_TO_FETCH,
                                        MAX_RPC_OUTSTANDING_EVENTS / jobConf.getNumReduceTasks());
        int maxEventsToFetch = Math.min(MAX_EVENTS_TO_FETCH, eventsPerReducer);
        
        // Start the map-completion events fetcher thread
        final EventFetcher<K, V> eventFetcher = new EventFetcher<K, V>(reduceId, umbilical, scheduler,
                                                                       this, maxEventsToFetch);
        eventFetcher.start();
        
        // Wait for shuffle to complete successfully
        while (!scheduler.waitUntilDone(PROGRESS_FREQUENCY)) {
            MapHost host = null;
            try {
                // Get a host to shuffle from
                host = scheduler.getHost();
                // Null Shuffle
                List<TaskAttemptID> maps = scheduler.getMapsForHost(host);
                for (TaskAttemptID mapId : maps) {
                    mergeMapOutput(mapId);
                    scheduler.copySucceeded(mapId, host, 0, 0, 0, new SharedFsMapOutput<K, V>(mapId, 0));
                }
            } finally {
                if (host != null) {
                    scheduler.freeHost(host);
                }
            }
            synchronized (this) {
                if (throwable != null) {
                    throw new Shuffle.ShuffleError("error in shuffle in " + throwingThreadName, throwable);
                }
            }
        }
        
        // Stop the event-fetcher thread
        eventFetcher.shutDown();
        
        // stop the scheduler
        scheduler.close();
        
        copyPhase.complete(); // copy is already complete
        taskStatus.setPhase(TaskStatus.Phase.SORT);
        reduceTask.statusUpdate(umbilical);
        
        RawKeyValueIterator kvIter = null;
        try {
            kvIter = finish();
        } catch (Throwable e) {
            throw new Shuffle.ShuffleError("Error while doing final merge ", e);
        }
        
        // Sanity check
        synchronized (this) {
            if (throwable != null) {
                throw new Shuffle.ShuffleError("error in shuffle in " + throwingThreadName, throwable);
            }
        }
        return kvIter;
    }
    
    public RawKeyValueIterator runLocal(Path[] localFiles) throws IOException, InterruptedException {
        for (Path file : localFiles) {
            addMapOutputSegments(file);
        }
        
        try {
            return finish();
        } catch (Throwable e) {
            throw new Shuffle.ShuffleError("Error while doing final merge ", e);
        }
    }
    
    public synchronized void addSegmentToMerge(Segment<K,V> segment) throws IOException {
        segmentsToBeMerged.add(segment);
        if (segmentsToBeMerged.size() >= (2 * ioSortFactor - 1)) {
            merger.startMerge(segmentsToBeMerged);
        }
    }
    
    @SuppressWarnings("unchecked")
    public RawKeyValueIterator finish() throws Throwable {
        // merge config params
        Class<K> keyClass = (Class<K>) jobConf.getMapOutputKeyClass();
        Class<V> valueClass = (Class<V>) jobConf.getMapOutputValueClass();
        final RawComparator<K> comparator = (RawComparator<K>) jobConf.getOutputKeyComparator();
        
        // Wait for on-going merges to complete
        merger.close();
        
        LOG.info("finalMerge called with " + segmentsToBeMerged.size() + " on-disk map-outputs");
        
        List<Segment<K, V>> segments = new ArrayList<Segment<K, V>>();
        long onDiskBytes = 0;
        
        for (Segment<K, V> segment : segmentsToBeMerged) {
            long fileLength = segment.getLength();
            onDiskBytes += fileLength;
            LOG.debug("Disk file: " + segment + " Length is " + fileLength);
            segments.add(segment);
        }
        segmentsToBeMerged.clear();
        
        LOG.info("Merging " + segmentsToBeMerged.size() + " files, " + onDiskBytes + " bytes from disk");
        Collections.sort(segments, new Comparator<Segment<K, V>>() {
            
            public int compare(Segment<K, V> o1, Segment<K, V> o2) {
                if (o1.getLength() == o2.getLength()) {
                    return 0;
                }
                return o1.getLength() < o2.getLength() ? -1 : 1;
            }
        });
        return Merger.merge(jobConf, lustrefs, keyClass, valueClass, segments, segments.size(), mergeTempDir,
                            comparator, reporter, spilledRecordsCounter, null, null);
    }
    
    public synchronized void reportException(Throwable t) {
        if (throwable == null) {
            throwable = t;
            throwingThreadName = Thread.currentThread().getName();
            // Notify the scheduler so that the reporting thread finds the
            // exception immediately.
            synchronized (scheduler) {
                scheduler.notifyAll();
            }
        }
    }
    
    private class FileMerger extends LustreFsMergeThread<Segment<K,V>, K, V> {
        
        private int numPasses = 0;
        
        public FileMerger() {
            super(ioSortFactor, LustreFsShuffle.this);
            setName("OnDiskMerger - Thread to merge on-disk map-outputs");
            setDaemon(true);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void merge(List<Segment<K,V>> segments) throws IOException {
            // sanity check
            if (segments == null || segments.isEmpty()) {
                LOG.info("No ondisk files to merge...");
                return;
            }
            
            Class<K> keyClass = (Class<K>) jobConf.getMapOutputKeyClass();
            Class<V> valueClass = (Class<V>) jobConf.getMapOutputValueClass();
            final RawComparator<K> comparator = (RawComparator<K>) jobConf.getOutputKeyComparator();
            
            long approxOutputSize = 0;
            int bytesPerSum = jobConf.getInt("io.bytes.per.checksum", 512);
            
            LOG.info("OnDiskMerger: We have  " + segments.size()
                     + " map outputs on disk. Triggering merge...");
            
            // 1. Prepare the list of files to be merged.
            for (Segment<K,V> segment : segments) {
                approxOutputSize += segment.getLength();
            }
            
            // add the checksum length
            approxOutputSize += ChecksumFileSystem.getChecksumLength(approxOutputSize, bytesPerSum);
            
            // 2. Start the on-disk merge process
            Path outputPath = new Path(reduceDir, "file-" + (numPasses++)).suffix(Task.MERGED_OUTPUT_PREFIX);
            
            Writer<K, V> writer = new Writer<K, V>(jobConf, lustrefs.create(outputPath),
                                                   (Class<K>) jobConf.getMapOutputKeyClass(), 
                                                   (Class<V>) jobConf.getMapOutputValueClass(),
                                                   codec, null, true);
            RawKeyValueIterator iter = null;
            try {
                iter = Merger.merge(jobConf, lustrefs, keyClass, valueClass, segments, ioSortFactor, mergeTempDir,
                                    comparator, reporter, spilledRecordsCounter, mergedMapOutputsCounter, null);
                Merger.writeFile(iter, writer, reporter, jobConf);
                writer.close();
            } catch (IOException e) {
                lustrefs.delete(outputPath, true);
                throw e;
            }
            addSegmentToMerge(new Segment<K, V>(jobConf, lustrefs, outputPath, codec, false, null));
            LOG.info(reduceId + " Finished merging " + segments.size()
                     + " map output files on disk of total-size " + approxOutputSize + "."
                     + " Local output file is " + outputPath + " of size "
                     + lustrefs.getFileStatus(outputPath).getLen());
        }
    }    
    
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    static class SharedFsMapOutput<K, V> extends MapOutput<K, V> {
        
        public SharedFsMapOutput(TaskAttemptID mapId, long size) throws IOException {
            
            super(mapId, size, true);
        }
        
        @Override
        public void shuffle(MapHost host, InputStream input, long compressedLength,
                            long decompressedLength, ShuffleClientMetrics metrics, Reporter reporter) throws IOException {
        }
        
        @Override
        public void commit() throws IOException {
            
        }
        
        @Override
        public void abort() {
            
        }
        
        @Override
        public String getDescription() {
            return "SHARED";
        }
    }
}
