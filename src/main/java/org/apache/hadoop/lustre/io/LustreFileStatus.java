// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.lustre.io;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;


public class LustreFileStatus extends FileStatus {

  private static final Log LOG = LogFactory.getLog(LustreFileStatus.class);
  
  LustreFsDelegate fsDelegate;
  File file;
  
  private long stripeSize = 0;
  private int stripeCount = -1, stripeOffset = -1;

  public LustreFileStatus(Path p, File f, long blockSize, LustreFsDelegate fs) {

    super(f.length(), f.isDirectory(), 1, blockSize, f.lastModified(), 0, null, null, null, p);
    this.file = f;
    this.fsDelegate = fs;
  }
  
  public File getFile() {
    return file;
  }

  private boolean isPermissionLoaded() {
    return !super.getOwner().equals("");
  }
  
  public void setPermission(short permission) {
    setPermission(FsPermission.createImmutable(permission));
  }
  
  public void setPermission(FsPermission permission) {
    super.setPermission(permission);
  }

  @Override
  public FsPermission getPermission() {
    if (!isPermissionLoaded()) {
      loadPermissionInfo();
    }
    return super.getPermission();
  }
  
  public void setOwner(String owner) {
    super.setOwner(owner);
  }

  @Override
  public String getOwner() {
    if (!isPermissionLoaded()) {
      loadPermissionInfo();
    }
    return super.getOwner();
  }
  
  public void setGroup(String group) {
    super.setGroup(group);
  }

  @Override
  public String getGroup() {
    if (!isPermissionLoaded()) {
      loadPermissionInfo();
    }
    return super.getGroup();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (!isPermissionLoaded()) {
      loadPermissionInfo();
    }
    super.write(out);
  }

  protected void loadPermissionInfo() {
    try {
      fsDelegate.fstat(this);
    } catch(IOException e) {
      LOG.warn("Unable to load stat information for : " + getPath());
    }
    
  }
}

