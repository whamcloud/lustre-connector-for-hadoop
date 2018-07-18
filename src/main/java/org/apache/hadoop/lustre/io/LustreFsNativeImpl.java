// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.lustre.io;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.EnumSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.HasFileDescriptor;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.nativeio.NativeIO;


public class LustreFsNativeImpl implements LustreFsDelegate {
  
  private static final Log LOG = LogFactory.getLog(LustreFsNativeImpl.class);
  
  public static final int POSIX_O_DIRECT = 00040000;
  
  public static final String LUSTRE_DIRECT_READ = "lustre.io.read.direct";
  public static final String LUSTRE_ASYNC_READ = "lustre.io.read.async";
  
  public static final String LUSTRE_DIRECT_WRITE = "lustre.io.write.direct";
  public static final String LUSTRE_ASYNC_WRITE = "lustre.io.write.async";
  
  
  public static final boolean available;  
  native public static void init();
  
  static {
    boolean initialized = false;
    if (NativeIO.isAvailable()) {
      try {
        System.loadLibrary("hadlustre");
        init();
        initialized = true;
      } catch (Throwable t) {
        //LOG.warn("Unable to initialize LustreNIO libraries");
      }
    }
    available = initialized;
  }
  
  static native FileDescriptor open(String path, int flags, int mode,
      long stripeSize, int stripeCount, int stripeOffset, String poolName) throws IOException;
  
  static native boolean close(FileDescriptor fd) throws IOException;
  
  private Configuration conf;
  
  public LustreFsNativeImpl(Configuration conf) {
	this.conf = conf;  
  }

  @Override
  public native boolean mkdirs(String path, short permissions) throws IOException;
  
  @Override
  public native boolean delete(String path, boolean recusrive) throws IOException;

  @Override
  public native void setstripe(String path, long stripeSize, int stripeCount, 
      int stripeOffset, String poolName) throws IOException;
      
  @Override
  public boolean rename(String src, String dest) throws IOException {
    NativeIO.renameTo(new File(src), new File(dest));
    return true;
  }  

  @Override
  public void fstat(LustreFileStatus status) throws IOException {
    String path = status.getFile().getAbsolutePath();
    NativeIO.POSIX.Stat stat = NativeIO.POSIX.getFstat(
        NativeIO.POSIX.open(path,NativeIO.POSIX.O_RDONLY, 0));
    status.setOwner(stat.getOwner());
    status.setGroup(stat.getGroup());
    status.setPermission((short) stat.getMode());
  }

  @Override
  public void chmod(String path, int mode) throws IOException {
	//TODO: implement natively
  }
  
  @Override
  public void chown(String path, String username, String groupname) throws IOException {
	//TODO: implement natively
  }
  
  @Override
  public InputStream open(String path, Statistics st, int bufsize) throws IOException {
	  int nFlags = NativeIO.POSIX.O_RDONLY;	  	  
	  if(conf.getBoolean(LUSTRE_ASYNC_READ, false)) {
		  nFlags |= (conf.getBoolean(LUSTRE_DIRECT_READ, false) ? POSIX_O_DIRECT : 0);
		  return new AsyncFileInputStream(
			open(path, nFlags, (short)0, 0L, 0, -1, null), st, bufsize);
	  }
	  else {
		  return new BufferedFileInputStream(
			open(path, nFlags, (short)0, 0L, 0, -1, null), st, bufsize);
	  }
  }
  
  public OutputStream open(String path, EnumSet<CreateFlag> flags, 
			short mode, boolean recursive, int bufsize,
			long stripeSize, int stripeCount, int stripeOffset, 
			String poolName) throws IOException {

	int nFlags = NativeIO.POSIX.O_WRONLY;	
	nFlags |= flags.contains(CreateFlag.CREATE) ? NativeIO.POSIX.O_CREAT : 0;
	nFlags |= flags.contains(CreateFlag.OVERWRITE) ? NativeIO.POSIX.O_TRUNC : 0;
	nFlags |= flags.contains(CreateFlag.APPEND) ? NativeIO.POSIX.O_APPEND : 0;
		
//    return new AsyncFileOutputStream(open(path,nFlags, mode, 
//    	stripeSize, stripeCount, stripeOffset, poolName), bufsize);
	return new BufferedOutputStream(new FileOutputStream(open(path,nFlags, mode, 
		stripeSize, stripeCount, stripeOffset, poolName)));
  }
}

abstract class MemUtils {
  public static native ByteBuffer allocate(int size);
  public static native int free(ByteBuffer buffer);
}

class AsyncFileInputStream extends InputStream
    implements Seekable, PositionedReadable, HasFileDescriptor {

  private Statistics statistics = null;
  
  private int _nativePage;
  protected ByteBuffer _nativeBuf;
  protected FileDescriptor file;
  private boolean pending = false;
  private IOException exception = null;

  protected int page;
  private boolean bufValid;
  
  private native long size(FileDescriptor fd) throws IOException;
  private native int read(FileDescriptor fd, long position, int count) throws IOException;

  public AsyncFileInputStream(FileDescriptor fd, Statistics st, int bufsize) throws IOException {
    if(bufsize <= 0 || bufsize % 4096 != 0) {
      throw new IllegalArgumentException("buffer size must be a multiple of 4K");
    }
    _nativeBuf = MemUtils.allocate(bufsize);
    file = fd;
    _nativePage = -1;    
    statistics = st;
    bufValid = false;
    next((page = 0));
  }

  @Override
  public synchronized int available() throws IOException {
    return _nativeBuf.remaining();
  }
  
  public synchronized long size() throws IOException {
    return size(file);
  }
  
  public void close() throws IOException {
    if(_nativeBuf == null) {
      throw new IOException("Stream Closed");
    }
    LustreFsNativeImpl.close(file);
    _nativeBuf = null;
  }

  public synchronized int read() throws IOException {
    return refill() ? (_nativeBuf.get() & 0xFF) : -1;
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
      int remain = Math.min(_nativeBuf.remaining(), len);
      _nativeBuf.get(b, off, remain);
      off += remain;
      len -= remain;
      totalBytesRead += remain;
    }
    return totalBytesRead;
  }

  @Override
  public synchronized long skip(long n) throws IOException {
	long position = getPos(); 
	long skipped = size() - position;
	skipped = n < skipped ? n : skipped;
	seek(position + skipped);	
    return skipped;
  }

  boolean refill() throws IOException {
    if (_nativeBuf == null)
      throw new IOException("Stream closed.");
    if (_nativeBuf.remaining() == 0 || !bufValid) {
      // if buffer is filled less than capacity, it means there are 
      // no more pages to fetch.
      if (bufValid && _nativeBuf.limit() < _nativeBuf.capacity()) {
        return false;
      }
      //_nativeBuf.position(_nativeBuf.position() % _nativeBuf.capacity());
      // if buffer was invalidated, reload current
      // page, otherwise go to next page.
      int loadPage = bufValid ? (page + 1) : page;
      fetch(loadPage);
      if (_nativeBuf.limit() > 0) {
        bufValid = true;
        page = loadPage;
        if (statistics != null) {
          statistics.incrementBytesRead(_nativeBuf.limit());
          statistics.incrementReadOps(1);
        }
      }
    }
    return (_nativeBuf.remaining() > 0);
  }
  
  private synchronized void complete(int count) {
    if(count < 0) {
      _nativeBuf.clear();
      exception = new IOException("Error reading file");
    }
    else _nativeBuf.limit(count);
    pending = false;
    notify();
  }
  
  private synchronized void error(IOException ioe) {
    _nativeBuf.limit(0);
    exception = ioe;
    pending = false;
    notify();
  }
  
  private synchronized void next(int page) throws IOException {
    if(pending) return;
    _nativePage = page;
    read(file, (_nativeBuf.capacity() * ((long)page)), _nativeBuf.capacity());
    pending = true;
  }
  
  private synchronized void fetch(int page) throws IOException {    
    try {
      while(pending) {
        wait();
      }
    } catch(InterruptedException ie) { }
    if(exception != null)
      throw exception;
    if(_nativePage != page) {
      next(page);
      fetch(page);
    }
  }

  @Override
  public synchronized int read(long position, byte[] buffer, int offset, int length) throws IOException {
    long originalPosition = getPos();
    seek(position);
    int nread = read(buffer, offset, length);
    seek(originalPosition);
    return nread;
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
	long originalPosition = getPos();
	seek(position);
	if(read(buffer, offset, length) < length)
	  throw new EOFException("End of file reached before reading fully.");
	seek(originalPosition);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    int page = (int) (pos / _nativeBuf.capacity());
    _nativeBuf.position((int) (pos % _nativeBuf.capacity()));
    // invalidate buffer only if page is changing.
    if (page != this.page) {
      this.page = page;
      bufValid = false;
      next(page);
    }
  }

  @Override
  public synchronized long getPos() throws IOException {
    return (((long) page) * _nativeBuf.capacity() + _nativeBuf.position());
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public FileDescriptor getFileDescriptor() throws IOException {
    return file;
  }
}

class AsyncFileOutputStream extends OutputStream {
  
  private long position = 0;
  private ByteBuffer _nativeBuf;  
  private boolean pending = false;
  protected FileDescriptor file;
  private IOException exception = null;
  
  private native int write(FileDescriptor fd, long position, int count) throws IOException;
  
  public AsyncFileOutputStream(FileDescriptor fd, int bufsize) {
    if(bufsize == 0 || bufsize % 4096 != 0) {
      throw new IllegalArgumentException("buffer size must be a multiple of 4K");
    }
    file = fd;
    _nativeBuf = MemUtils.allocate(bufsize);
  }    

  public synchronized void close() throws IOException {
    if(_nativeBuf == null) {
      throw new IOException("Stream closed.");
    }
    pending();
    _nativeBuf.limit(_nativeBuf.position());
    flush();
    LustreFsNativeImpl.close(file);
    _nativeBuf = null;
  }
  
  private synchronized void complete(int count) {
    if(count < 0) {      
      exception = new IOException("Error writing file");
    }
    pending = false;
    notify();
  }
  
  private synchronized void error(IOException ioe) {
    exception = ioe;
    pending = false;
    notify();
  }
  
  private synchronized void pending() throws IOException {
    boolean hadToWait = pending;
    try {
      while(pending) {
        wait();
        if(exception != null) {
          throw exception;
        }
      }
    } catch(InterruptedException ie) { }
    if(hadToWait) _nativeBuf.clear();
  }
  
  @Override
  public synchronized void flush() throws IOException {
    if(_nativeBuf == null) {
      throw new IOException("Stream closed.");
    }
    int length = _nativeBuf.limit();
    if(_nativeBuf.remaining() == 0 && length > 0) {
      pending = true;
      if(write(file, position, length) != 0)
        throw new IOException("Write failed");            
      position += length; 
    }    
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    if (off < 0 || len < 0 || b.length - off < len)
      throw new IndexOutOfBoundsException();

    if (len == 0)
      return;

    while (len > 0) {      
      pending();
      int remain = Math.min(_nativeBuf.remaining(), len);
      _nativeBuf.put(b, off, remain);
      off += remain;
      len -= remain;
      flush();      
    } 
  }

  @Override
  public synchronized void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public synchronized void write(int b) throws IOException {
    pending();
    _nativeBuf.put((byte)(b & 0xFF));
    flush();   
  }
}
