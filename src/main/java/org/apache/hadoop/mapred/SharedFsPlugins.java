// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LustreFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.task.reduce.LustreFsShuffle;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.PureJavaCrc32;

public class SharedFsPlugins {

  public static final String PATH = ".cache/%s/";
  public static final String MAP_OUTPUT = "reduce-%d/map-%d_spill-%d.out";
  public static final int MAP_OUTPUT_INDEX_RECORD_LENGTH = 24;

  public static Path getTempPath(Configuration conf, JobID jobid)
      throws IOException {            
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    Path path = new Path(
        conf.get(MRJobConfig.MR_AM_STAGING_DIR,
        MRJobConfig.DEFAULT_MR_AM_STAGING_DIR)
        + Path.SEPARATOR + user + Path.SEPARATOR 
        + String.format(PATH, jobid));    
    return FileSystem.get(LustreFileSystem.NAME, conf).makeQualified(path);
  }
  
  // This method is required to access the package visible members of segment
  @SuppressWarnings("rawtypes")
  public static Path getSegmentPath(Segment segment) {
    return segment.file;
  }
  
  @InterfaceAudience.LimitedPrivate({"MapReduce"})
  @InterfaceStability.Unstable
  public static class SpillRecord {

    /** Backing store */
    private final ByteBuffer buf;
    /** View of backing storage as longs */
    private final LongBuffer entries;

    public SpillRecord(int numPartitions) {
      buf = ByteBuffer.allocate(
          numPartitions * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
      entries = buf.asLongBuffer();
    }

    public SpillRecord(Path indexFileName, JobConf job) throws IOException {
      this(indexFileName, job, null);
    }

    public SpillRecord(Path indexFileName, JobConf job, String expectedIndexOwner)
      throws IOException {
      this(indexFileName, job, new PureJavaCrc32(), expectedIndexOwner);
    }

    public SpillRecord(Path indexFileName, JobConf job, Checksum crc,
                       String expectedIndexOwner)
        throws IOException {

      final FileSystem rfs = FileSystem.getLocal(job).getRaw();
      final FSDataInputStream in =
          SecureIOUtils.openFSDataInputStream(new File(indexFileName.toUri()
              .getRawPath()), expectedIndexOwner, null);
      try {
        final long length = rfs.getFileStatus(indexFileName).getLen();
        final int partitions = (int) length / MAP_OUTPUT_INDEX_RECORD_LENGTH;
        final int size = partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH;
        buf = ByteBuffer.allocate(size);
        if (crc != null) {
          crc.reset();
          CheckedInputStream chk = new CheckedInputStream(in, crc);
          IOUtils.readFully(chk, buf.array(), 0, size);
          
          if (chk.getChecksum().getValue() != in.readLong()) {
            throw new ChecksumException("Checksum error reading spill index: " +
                                  indexFileName, -1);
          }
        } else {
          IOUtils.readFully(in, buf.array(), 0, size);
        }
        entries = buf.asLongBuffer();
      } finally {
        in.close();
      }
    }

    /**
     * Return number of IndexRecord entries in this spill.
     */
    public int size() {
      return entries.capacity() / (MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8);
    }

    /**
     * Get spill offsets for given partition.
     */
    public IndexRecord getIndex(int partition) {
      final int pos = partition * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
      return new IndexRecord(entries.get(pos), entries.get(pos + 1),
                             entries.get(pos + 2));
    }

    /**
     * Set spill offsets for given partition.
     */
    public void putIndex(IndexRecord rec, int partition) {
      final int pos = partition * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
      entries.put(pos, rec.startOffset);
      entries.put(pos + 1, rec.rawLength);
      entries.put(pos + 2, rec.partLength);
    }

    /**
     * Write this spill record to the location provided.
     */
    public void writeToFile(Path loc, JobConf job)
        throws IOException {
      writeToFile(loc, job, new PureJavaCrc32());
    }

    public void writeToFile(Path loc, JobConf job, Checksum crc)
        throws IOException {
      final FileSystem rfs = FileSystem.getLocal(job).getRaw();
      CheckedOutputStream chk = null;
      final FSDataOutputStream out = rfs.create(loc);
      try {
        if (crc != null) {
          crc.reset();
          chk = new CheckedOutputStream(out, crc);
          chk.write(buf.array());
          out.writeLong(chk.getChecksum().getValue());
        } else {
          out.write(buf.array());
        }
      } finally {
        if (chk != null) {
          chk.close();
        } else {
          out.close();
        }
      }
    }

  }

  public static class MapOutputBuffer<K, V> extends LustreFsOutputCollector<K, V> {
  }

  public static class Shuffle<K, V> extends LustreFsShuffle<K, V> {
  }
}
