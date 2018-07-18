// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.lustre.io;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;


public class LustreFsJavaImpl implements LustreFsDelegate {
  
  private static final Log LOG = LogFactory.getLog(LustreFsJavaImpl.class);
  
  public static final String MAP_OUTPUT = "reduce-%d/map-%d_spill-%d.out";  

  @Override
  public boolean mkdirs(String path, short permissions) throws IOException {
    File f = new File(path), p = f;
    while(!p.getParentFile().exists()) {
    	p = p.getParentFile();
    }
    if(!p.getParentFile().isDirectory()) {
      throw new FileAlreadyExistsException("Not a directory: "+ p.getParent());
    }
    boolean success = f.mkdirs();
    if (success) {
      if(-1 != permissions) {
    	chmod(path, permissions);
      }
      // We give explicit permissions to the user who submitted the job using ACLs
      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      LinkedList<String> args = new LinkedList<String>();
      args.add("/usr/bin/setfacl");
      args.add("-R");
      args.add("-m");
      args.add("u:" + user + ":" + FsAction.ALL.SYMBOL);
      args.add(FileUtil.makeShellPath(p, true));	
      org.apache.hadoop.fs.util.Shell.runPrivileged(args.toArray(new String[0]));
      args.add(2, "-d");
      org.apache.hadoop.fs.util.Shell.runPrivileged(args.toArray(new String[0]));
    }
    return (success || (f.exists() && f.isDirectory()));
  }
  
  @Override
  public void chown(String path, String username, String groupname) throws IOException {
	java.nio.file.Path p = FileSystems.getDefault().getPath(path);
	PosixFileAttributeView view = Files.getFileAttributeView(p, PosixFileAttributeView.class);
	UserPrincipalLookupService service = p.getFileSystem().getUserPrincipalLookupService();
	if(!StringUtils.isBlank(username)) {
		view.setOwner(service.lookupPrincipalByName(username));
	}
	if(!StringUtils.isBlank(groupname)) {
		view.setGroup(service.lookupPrincipalByGroupName(groupname));
	}
  }

  @Override
  public boolean rename(String src, String dest) throws IOException {
    return new File(src).renameTo(new File(dest));
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    File f = new File(path);
    if (f.isFile()) {
      return f.delete();
    } else if (!recursive && f.isDirectory() && (FileUtil.listFiles(f).length != 0)) {
      throw new IOException("Directory " + f.toString() + " is not empty");
    }
    return FileUtil.fullyDelete(f);
  }

  @Override
  public void fstat(LustreFileStatus status) throws IOException {
    IOException e = null;
    try {

      String[] args = {"/usr/bin/getfacl", FileUtil.makeShellPath(status.getFile(), true)};
      
      String pattern = "owner: ([^\n]+).*group: ([^\n]+)" +
			".*^user::([^\\s]+).*^group::([^\\s]+).*^other::([^\\s]+)";
      Matcher m = Pattern.compile(pattern, Pattern.MULTILINE | Pattern.DOTALL)
				.matcher(org.apache.hadoop.fs.util.Shell.runPrivileged(args));
      m.find();
      status.setOwner(m.group(1));
      status.setGroup(m.group(2));    
      String perms = "-"+m.group(3)+m.group(4)+m.group(5);
      status.setPermission(FsPermission.valueOf(perms));      
    } catch (Shell.ExitCodeException ioe) {
      if (ioe.getExitCode() != 1) {
        e = ioe;
      }
    } catch (IOException ioe) {
      e = ioe;
    } finally {
      if (e != null) {
        throw new RuntimeException("Error while running command to get " + "file permissions : "
            + org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    }
  }

  @Override
  public void chmod(String path, int mode) throws IOException {
	File f = new File(path);
	FsPermission perm = FsPermission.createImmutable((short)mode);
	LinkedList<String> args = new LinkedList<String>();
	args.add("/usr/bin/setfacl");
	args.add("-m");
	args.add(
		"u::" + perm.getUserAction().SYMBOL +
		",g::" + perm.getGroupAction().SYMBOL +
		",o::" + perm.getOtherAction().SYMBOL);
	args.add(FileUtil.makeShellPath(f, true));	
    org.apache.hadoop.fs.util.Shell.runPrivileged(args.toArray(new String[0]));
    
    // Set default acls on directories so children can inherit.
    if(f.isDirectory()) {
    	args.add(1, "-d");
    	org.apache.hadoop.fs.util.Shell.runPrivileged(args.toArray(new String[0]));
    }
  }
  
  @Override
  public InputStream open(String path, Statistics st, int bufsize) throws IOException {
    return new BufferedFileInputStream(new File(path), st, bufsize);
  }
  
  public OutputStream open(String path, EnumSet<CreateFlag> flags, 
		  					short mode, boolean recursive, int bufsize,
		  					long stripeSize, int stripeCount, int stripeOffset, 
		  					String poolName) throws IOException {
	  
	File f = new File(path);
    boolean exists = f.exists();    
    CreateFlag.validate(path, exists, flags);
    
    if(!exists) {
      File parent = f.getParentFile();
      if(!(recursive || parent.exists())) {
    	  throw new IOException("Parent directory does not exist: " + parent + ".");
      }
      else if (!mkdirs(parent.getAbsolutePath(), (short)-1)) {
        throw new IOException("Mkdirs failed to create " + parent);        
      }
      // Stripe must be set first. Cannot be set on an existing file.
      setstripe(path, stripeSize, stripeCount, stripeOffset, poolName);
    }
    OutputStream out = new FileOutputStream(f, flags.contains(CreateFlag.APPEND));
    if (mode != (short) -1) {
      chmod(path, mode);
    }
    return new BufferedOutputStream(out, bufsize);    
  }

  @Override
  public void setstripe(String path, long stripeSize, int stripeCount, int stripeOffset,
      String poolName) throws IOException {

    int argIndex = 2;
    String[] args;

    if (!(poolName == null || poolName.equals(""))) {
      args = new String[11];
      args[argIndex++] = "-p";
      args[argIndex++] = poolName;
    } else
      args = new String[9];

    args[0] = "/usr/bin/lfs";
    args[1] = "setstripe";

    if (stripeSize < 0L) {
      stripeSize = (1024 * 1024);
    }
    args[argIndex++] = "-S";
    args[argIndex++] = String.valueOf(stripeSize);

    if (stripeCount < -1) {
      stripeCount = 0;
    }
    args[argIndex++] = "-c";
    args[argIndex++] = String.valueOf(stripeCount);

    if (stripeOffset < -1) {
      stripeOffset = -1;
    }
    args[argIndex++] = "-i";
    args[argIndex++] = String.valueOf(stripeOffset);

    args[argIndex++] = path;

    org.apache.hadoop.fs.util.Shell.runPrivileged(args);
  }
}
