// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileTime;
import java.util.EnumSet;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.local.RawLocalFs;
import org.apache.hadoop.fs.local.RawLocalProxyFs;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.util.Shell;
import org.apache.hadoop.lustre.io.LustreFileStatus;
import org.apache.hadoop.lustre.io.LustreFsDelegate;
import org.apache.hadoop.lustre.io.LustreFsJavaImpl;
import org.apache.hadoop.lustre.io.LustreFsNativeImpl;
import org.apache.hadoop.util.Progressable;

public class LustreFileSystem extends FileSystem {
  
  private static final Log LOG = LogFactory.getLog(LustreFileSystem.class);

  public static final String FS_IN_MEMORY_MERGE_KEY = "fs.merge.in.memory";
  public static final String FS_ROOT_CONF_KEY = "fs.root.dir";
  public static final String FS_BLOCK_SIZE_KEY = "fs.block.size";  

  public static final String LUSTRE_STRIPE_SIZE_KEY = "lustre.stripe.size";
  public static final String LUSTRE_STRIPE_COUNT_KEY = "lustre.stripe.count";
  public static final String LUSTRE_URI_SCHEME = "lustre";
  public static final URI NAME = URI.create("lustre:///");
  
  protected Path workingDir, rootDir, mountPoint;
  protected String fsName;  

  private String poolName = null;
  private long defaultStripeSize;
  private int defaultStripeCount;
  protected long defaultBlockSize;
  
  private LustreFsDelegate fsDelegate;  

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);

    setupLustrePaths(conf.get(FS_ROOT_CONF_KEY));
    
    this.defaultBlockSize = conf.getLong(FS_BLOCK_SIZE_KEY, 1073741824L);
    this.defaultStripeSize = conf.getLong(LUSTRE_STRIPE_SIZE_KEY, 1048576L);
    this.defaultStripeCount = conf.getInt(LUSTRE_STRIPE_COUNT_KEY, 1);
    
    fsDelegate = LustreFsNativeImpl.available ? new LustreFsNativeImpl(conf) : new LustreFsJavaImpl();
  }

  @Override
  public long getDefaultBlockSize() {
    return defaultBlockSize;
  }
  
  @Override
  public URI getUri() {
    return NAME;
  }

  @Override
  public String getScheme() {
    return LUSTRE_URI_SCHEME;
  }

  @Override
  public String toString() {
    return "LustreFS : " + rootDir.toUri().getPath();
  }
  
  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir.isAbsolute() ? newDir : new Path(workingDir, newDir);
    checkPath(workingDir);
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }


  public Path mapToLocal(Path path) {

    if (path == null)
      return null;

    // Make it relative to the file system root directory
    String rootPath = rootDir.toUri().getPath();
    String thePath = makeQualified(path).toUri().getPath();

    if (path.isAbsolute()) {
      path = new Path(thePath.replace(rootPath, "") + Path.SEPARATOR);
    } else {
      // If already relative, make it a child of working directory
      path = new Path(workingDir, thePath + Path.SEPARATOR);
    }

    // Map to the root directory of the file system, if not mapped already
    return new Path(rootPath + path.toUri().getPath());
  }

  /**
   * Convert a path to a file
   * 
   * @return a File representing the path in the file system.
   */
  // @Override
  public File pathToFile(Path path) {
    return new File(mapToLocal(path).toUri().getPath());
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    File localf = pathToFile(f);
    FileStatus[] results;

    if (!localf.exists()) {
      throw new FileNotFoundException("File " + f + " does not exist. File System : " + toString());
    }
    if (localf.isFile()) {
      return new FileStatus[]{getFileStatus(f)};
    }

    String[] names = localf.list();
    if (names == null) {
      return null;
    }
    results = new FileStatus[names.length];
    for (int i = 0; i < names.length; i++) {
      results[i] = getFileStatus(new Path(f, names[i]));
    }
    return results;
  }    
  
  /**
   * Returns a status object describing the use and capacity of the
   * file system. If the file system has multiple partitions, the
   * use and capacity of the partition pointed to by the specified
   * path is reflected.
   * @param p Path for which status should be obtained. null means
   * the default partition. 
   * @return a FsStatus object
   * @throws IOException
   *           see specific implementation
   */
  @Override
  public FsStatus getStatus(Path p) throws IOException {
    File partition = pathToFile(p == null ? new Path("/") : p);
    //File provides getUsableSpace() and getFreeSpace()
    //File provides no API to obtain used space, assume used = total - free
    return new FsStatus(partition.getTotalSpace(), 
      partition.getTotalSpace() - partition.getFreeSpace(),
      partition.getUsableSpace());
  }
  
  /**
   * Set permission of a path.
   * @param p
   * @param permission
   */
  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
	  fsDelegate.chmod(pathToFile(p).getAbsolutePath(), permission.toShort());
  }

  /**
   * Set owner of a path (i.e. a file or a directory).
   * The parameters username and groupname cannot both be null.
   * @param p The path
   * @param username If it is null, the original username remains unchanged.
   * @param groupname If it is null, the original groupname remains unchanged.
   */
  @Override
  public void setOwner(Path path, String username, String groupname) throws IOException {
	fsDelegate.chown(pathToFile(path).getAbsolutePath(), username, groupname);
  }

  /**
   * Set access time of a file
   * @param p The path
   * @param mtime Set the modification time of this file.
   *              The number of milliseconds since Jan 1, 1970. 
   *              A value of -1 means that this call should not set modification time.
   * @param atime Set the access time of this file.
   *              The number of milliseconds since Jan 1, 1970. 
   *              A value of -1 means that this call should not set access time.
   */
  @Override
  public void setTimes(Path path, long mtime, long atime) throws IOException {	  
	java.nio.file.Path p = FileSystems.getDefault().getPath(pathToFile(path).getAbsolutePath());
	BasicFileAttributeView view = Files.getFileAttributeView(p, BasicFileAttributeView.class);	  
	view.setTimes(mtime == -1 ? null : FileTime.fromMillis(mtime),
	  atime == -1 ? null : FileTime.fromMillis(atime), null);
  }
  
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    String file = mapToLocal(f).toUri().getPath();
    return new FSDataInputStream(fsDelegate.open(file, statistics, bufferSize));
  }
  
   @Deprecated
   @Override
   public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
       EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
       Progressable progress) throws IOException {
	   String file = mapToLocal(f).toUri().getPath();
	   return new FSDataOutputStream(
		        fsDelegate.open(file, flags, permission.toShort(), false, bufferSize, 
		        		getDefaultStripeSize(), getDefaultStripeCount(), -1, null), statistics);
   }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    String file = mapToLocal(f).toUri().getPath();
    return new FSDataOutputStream(
        fsDelegate.open(file, overwrite ? EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
                : EnumSet.of(CreateFlag.CREATE), permission.toShort(), true, 
        		bufferSize, getDefaultStripeSize(), getDefaultStripeCount(), -1, null), statistics);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    String file = mapToLocal(f).toUri().getPath();
    return new FSDataOutputStream(
            fsDelegate.open(file, EnumSet.of(CreateFlag.APPEND), (short)-1, true, 
            		bufferSize, getDefaultStripeSize(), getDefaultStripeCount(), -1, null), statistics);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return fsDelegate.rename(mapToLocal(src).toUri().getPath(), mapToLocal(dst).toUri().getPath());
  }

  @Override
  public boolean delete(Path p, boolean recursive) throws IOException {
    return fsDelegate.delete(mapToLocal(p).toUri().getPath(), recursive);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    String path = mapToLocal(f).toUri().getPath();
    return fsDelegate.mkdirs(path, permission.toShort());
  }

  public long getDefaultStripeSize() {
    return defaultStripeSize;
  }

  public int getDefaultStripeCount() {
    return defaultStripeCount;
  }

  public void setStripe(Path path, long stripeSize, int stripeCount, int stripeOffset) throws IOException {
    fsDelegate.setstripe(mapToLocal(path).toUri().getPath(), 
        stripeSize, stripeCount, stripeOffset, poolName);
  }

  public FileStatus getFileStatus(Path path) throws IOException {
    File file = pathToFile(path);
    if (!file.exists()) {
      throw new FileNotFoundException("File " + path + " does not exist. File System : " + toString());
    }
    return new LustreFileStatus(makeQualified(path), file, getDefaultBlockSize(), fsDelegate);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
      throws IOException {

    if ((start < 0) || (len < 0)) {
      throw new IllegalArgumentException("Invalid start or len parameter");
    }

    if (file.getLen() <= start) {
      return new BlockLocation[0];
    }

    long blockSize = getDefaultBlockSize();
    start = 0;
    len = file.getLen();

    BlockLocation[] result = new BlockLocation[(int) Math.ceil(len / (double) blockSize)];

    for (int i = 0; i < result.length; i++) {
      long length = len - start;
      if (length > blockSize)
        length = blockSize;
      result[i] = new BlockLocation(null, new String[]{"localhost"}, start, length);
      start += length;
    }
    return result;
  }
  
  protected void setupLustrePaths(final String path) throws IOException {
    // check if configured
    if (null == path) {
      throw new IOException("Root directory not configured. [" + FS_ROOT_CONF_KEY + "] ");
    }

    // check if valid directory
    if (!new File(path).isDirectory()) {
      throw new IOException("Root path: " + path + " does not exist or is not a directory");
    }

    String output = Shell.runPrivileged(new String[]{ "/bin/df", "-Th", path });
    // skip first line, contains headers
    output = output.substring(output.indexOf("\n"));
    StringTokenizer st = new StringTokenizer(output);

    fsName = st.nextToken();    
    
    // column Type
    if (!LUSTRE_URI_SCHEME.equals(st.nextToken()))
      throw new IOException("File system type for : " + path + " is not " + LUSTRE_URI_SCHEME);

    // get the last token -- mount point
    String lastToken = Path.SEPARATOR;
    while (st.hasMoreTokens())
      lastToken = st.nextToken();
    
    mountPoint = new Path(lastToken);
    rootDir = new Path(path);
    workingDir = new Path("/");
    
    // Remove MDS NID and leading '/' to get actual fsName
    fsName = fsName.substring(fsName.indexOf(":")+2);
    ////poolName = getPoolName();
  }
  
  protected String getPoolName() {    
    String hostname = "";
    try {
      hostname = Shell.runPrivileged(
          new String[]{"/bin/hostname","-s"}).trim();
      // if FQDN, shorten it. (just in case).
      int dotIndex = hostname.indexOf(".");
      if(dotIndex != -1) hostname = hostname.substring(dotIndex+1);      
      String poolName = fsName+".had-"+hostname;      
      String poolList = Shell.runPrivileged(
          new String[]{"/usr/sbin/lctl","pool_list",fsName});
      StringTokenizer st = new StringTokenizer(poolList);
      // Ignore header
      st.nextToken("\n");
      while(st.hasMoreTokens()) {
        String line = st.nextToken();
        if(line.equals(poolName)) {
          LOG.info("Found a local pool: " + poolName);
          return line;
        }
      }
    }
    catch(IOException e) {
      LOG.warn("getPoolName: " + e.getMessage());      
    }    
    return null;
  }
  
  public static class LustreFs extends DelegateToFileSystem {

    private RawLocalFs proxyFs = null;

    /**
     * This constructor has the signature needed by
     * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}.
     * 
     * @param theUri
     *          which must be that of Lustre
     * @param conf
     * @throws IOException
     * @throws URISyntaxException
     */
    LustreFs(final URI theUri, final Configuration conf) throws IOException, URISyntaxException {

      super(theUri, new LustreFileSystem(), conf, LustreFileSystem.NAME.getScheme(), false);
      try {
        proxyFs = new RawLocalProxyFs(conf);
      } catch (UnsupportedFileSystemException e) {
        throw new IOException("Local Unsupported??", e);
      }
    }

    private Path mapToLocal(Path p) {
      return ((LustreFileSystem) fsImpl).mapToLocal(p);
    }

    /**
     * Wrappers
     */

    @Override
    public int getUriDefaultPort() {
      return proxyFs.getUriDefaultPort();
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
      return proxyFs.getServerDefaults();
    }

    @Override
    public boolean supportsSymlinks() {
      return proxyFs.supportsSymlinks();
    }

    @Override
    public void createSymlink(Path target, Path link, boolean createParent) throws IOException {
      proxyFs.createSymlink(mapToLocal(target), link, createParent);
    }

    @Override
    public FileStatus getFileLinkStatus(final Path f) throws IOException {
      return proxyFs.getFileLinkStatus(mapToLocal(f));
    }

    @Override
    public Path getLinkTarget(Path f) throws IOException {
      return proxyFs.getLinkTarget(mapToLocal(f));
    }
  }
}
