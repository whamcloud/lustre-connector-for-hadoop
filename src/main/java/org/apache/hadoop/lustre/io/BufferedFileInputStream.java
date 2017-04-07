// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.lustre.io;

import java.io.EOFException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.HasFileDescriptor;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

class BufferedFileInputStream extends InputStream
    implements Seekable, PositionedReadable, HasFileDescriptor {

  private Statistics statistics = null;

  protected byte[] buf;
  protected int count;
  protected int offset;
  protected InputStream in;
  protected int page;
  private boolean bufValid;
  
  private BufferedFileInputStream(Statistics st, int bufsize) throws IOException {
    if (bufsize <= 0)
      throw new IllegalArgumentException();
    buf = new byte[bufsize];
    offset = count = bufsize;
    statistics = st;
    bufValid = false;
    page = 0;
  }
  
  public BufferedFileInputStream(File f, Statistics st, int bufsize) throws IOException {
    this(st, bufsize);
    this.in = new FileInputStream(f);    
  }

  public BufferedFileInputStream(FileDescriptor fd, Statistics st, int bufsize) throws IOException {
    this(st, bufsize);
    this.in = new FileInputStream(fd);    
  }

  @Override
  public synchronized int available() throws IOException {
    return count - offset + in.available();
  }

  @Override
  public void close() throws IOException {
    buf = null;
    offset = count = 0;
    in.close();
  }

  public synchronized int read() throws IOException {
    return refill() ? (buf[offset++] & 0xFF) : -1;
  }

  public synchronized int read(byte[] b, int off, int len) throws IOException {
    if (off < 0 || len < 0 || b.length - off < len)
      throw new IndexOutOfBoundsException();

    if (len == 0)
      return 0;

    if (!refill())
      return -1; // No bytes were read before EOF.

    int totalBytesRead = 0;
    while (len > 0 && refill()) {
      int remain = Math.min(count - offset, len);
      System.arraycopy(buf, offset, b, off, remain);
      offset += remain;
      off += remain;
      len -= remain;
      totalBytesRead += remain;
    }
    return totalBytesRead;
  }

  @Override
  public synchronized long skip(long n) throws IOException {
	FileChannel channel = ((FileInputStream)in).getChannel();
	long position = getPos(); 
	long skipped = (channel.size() - position);
	skipped = n < skipped ? n : skipped;
	seek(position + skipped);	
    return skipped;
  }

  boolean refill() throws IOException {
    if (buf == null)
      throw new IOException("Stream closed.");
    if (offset >= count || !bufValid) {
      if (bufValid && count < buf.length) {
        return false;
      }
      offset %= buf.length;
      // if buffer was invalidated, reload current
      // page, otherwise go to next page.
      int loadPage = bufValid ? (page + 1) : page;
      long position = ((long) buf.length) * loadPage;
      int nread = read(position, buf, 0, buf.length);
      if (nread < 0) {
        count = 0;
      } else {
        bufValid = true;
        count = nread;
        page = loadPage;
        if (statistics != null) {
          statistics.incrementBytesRead(count);
          statistics.incrementReadOps(1);
        }
      }
    }
    return (offset < count);
  }
  
  private int read(long position, ByteBuffer buffer) throws IOException {
    return ((FileInputStream) in).getChannel().read(buffer, position);
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    return read(position, ByteBuffer.wrap(buffer, offset, length));
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    int nread = 0;
    while (nread < length) {
      int nbytes = read(position + nread, buffer, offset + nread, length - nread);
      if (nbytes < 0) {
        throw new EOFException("End of file reached before reading fully.");
      }
      nread += nbytes;
    }
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  @Override
  public void seek(long pos) throws IOException {
    int page = (int) (pos / buf.length);
    offset = (int) (pos % buf.length);
    // invalidate buffer only if page is changing.
    if (page != this.page) {
      this.page = page;
      bufValid = false;
    }
  }

  @Override
  public long getPos() throws IOException {
    return (((long) page) * buf.length + offset);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public FileDescriptor getFileDescriptor() throws IOException {
    return ((FileInputStream) in).getFD();
  }
}
