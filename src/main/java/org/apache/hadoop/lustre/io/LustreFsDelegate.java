// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.lustre.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.EnumSet;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem.Statistics;

public interface LustreFsDelegate {

  boolean mkdirs(String path, short permissions) throws IOException;

  boolean rename(String src, String dest) throws IOException;

  boolean delete(String path, boolean recusrive) throws IOException;

  void fstat(LustreFileStatus status) throws IOException;

  void chmod(String path, int mode) throws IOException;
  
  void chown(String path, String username, String groupname) throws IOException;

  InputStream open(String path, Statistics st, int bufsize) throws IOException;

  public OutputStream open(String path, EnumSet<CreateFlag> flags, 
			short mode, boolean recursive, int bufsize,
			long stripeSize, int stripeCount, int stripeOffset, 
			String poolName) throws IOException;

  void setstripe(String path, long stripeSize, int stripeCount, 
		  int stripeOffset, String poolName) throws IOException;
}
