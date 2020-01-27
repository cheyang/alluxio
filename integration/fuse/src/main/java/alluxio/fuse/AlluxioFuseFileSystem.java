/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.fuse;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.FileIncompleteException;
import alluxio.exception.OpenDirectoryException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.MasterClientContext;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.BlockMasterInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import jnr.ffi.Pointer;
import jnr.ffi.types.gid_t;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnr.ffi.types.uid_t;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseContext;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Statvfs;
import ru.serce.jnrfuse.struct.Timespec;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Main FUSE implementation class.
 *
 * Implements the FUSE callbacks defined by jnr-fuse.
 */
@ThreadSafe
public final class AlluxioFuseFileSystem extends FuseStubFS {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFuseFileSystem.class);
  private static final int MAX_OPEN_FILES = Integer.MAX_VALUE;
  private static final int MAX_OPEN_WAITTIME_MS = 5000;
  private static final String ramDiskDIR = "/mnt/ramdisk";
  private static final String mntPoint = "/alluxio-fuse";
  private final Map<String, Integer> fileContents;
  /**
   * df command will treat -1 as an unknown value.
   */
  @VisibleForTesting
  public static final int UNKNOWN_INODES = -1;
  /**
   * Most FileSystems on linux limit the length of file name beyond 255 characters.
   */
  @VisibleForTesting
  public static final int MAX_NAME_LENGTH = 255;

  private static InstancedConfiguration sConf =
      new InstancedConfiguration(ConfigurationUtils.defaults());

  /**
   * 4294967295 is unsigned long -1, -1 means that uid or gid is not set.
   * 4294967295 or -1 occurs when chown without user name or group name.
   * Please view https://github.com/SerCeMan/jnr-fuse/issues/67 for more details.
   */
  @VisibleForTesting
  public static final long ID_NOT_SET_VALUE = -1;
  @VisibleForTesting
  public static final long ID_NOT_SET_VALUE_UNSIGNED = 4294967295L;

  private static final long UID = AlluxioFuseUtils.getUid(System.getProperty("user.name"));
  private static final long GID = AlluxioFuseUtils.getGid(System.getProperty("user.name"));

  // Open file managements
  private static final IndexDefinition<OpenFileEntry, Long> ID_INDEX =
      new IndexDefinition<OpenFileEntry, Long>(true) {
        @Override
        public Long getFieldValue(OpenFileEntry o) {
          return o.getId();
        }
      };

  private static final IndexDefinition<OpenFileEntry, String> PATH_INDEX =
      new IndexDefinition<OpenFileEntry, String>(true) {
        @Override
        public String getFieldValue(OpenFileEntry o) {
          return o.getPath();
        }
      };

  private final boolean mIsUserGroupTranslation;
  private final FileSystem mFileSystem;
  // base path within Alluxio namespace that is used for FUSE operations
  // For example, if alluxio-fuse is mounted in /mnt/alluxio and mAlluxioRootPath
  // is /users/foo, then an operation on /mnt/alluxio/bar will be translated on
  // an action on the URI alluxio://<master>:<port>/users/foo/bar
  private final Path mAlluxioRootPath;
  // Keeps a cache of the most recently translated paths from String to Alluxio URI
  private final LoadingCache<String, AlluxioURI> mPathResolverCache;

  // Table of open files with corresponding InputStreams and OutputStreams
  private final IndexedSet<OpenFileEntry> mOpenFiles;

  private AtomicLong mNextOpenFileId = new AtomicLong(0);
  private final String mFsName;

  private final boolean noop = true;

  /**
   * Creates a new instance of {@link AlluxioFuseFileSystem}.
   *
   * @param fs Alluxio file system
   * @param opts options
   * @param conf Alluxio configuration
   */
  public AlluxioFuseFileSystem(FileSystem fs, AlluxioFuseOptions opts, AlluxioConfiguration conf) {
    super();
    mFsName = conf.get(PropertyKey.FUSE_FS_NAME);
    mFileSystem = fs;
    mAlluxioRootPath = Paths.get(opts.getAlluxioRoot());
    mOpenFiles = new IndexedSet<>(ID_INDEX, PATH_INDEX);

    final int maxCachedPaths = conf.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX);
    mIsUserGroupTranslation
        = conf.getBoolean(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED);
//    mPathResolverCache = CacheBuilder.newBuilder()
//        .maximumSize(maxCachedPaths)
//        .build(new PathCacheLoader());

    mPathResolverCache = null;
    Preconditions.checkArgument(mAlluxioRootPath.isAbsolute(),
        "alluxio root path should be absolute");

    fileContents = Maps.newHashMap();
  }

  /**
   * Changes the mode of an Alluxio file.
   *
   * @param path the path of the file
   * @param mode the mode to change to
   * @return 0 on success, a negative value on error
   */
  @Override
  public int chmod(String path, @mode_t long mode) {
    AlluxioURI uri = mPathResolverCache.getUnchecked(path);

    SetAttributePOptions options = SetAttributePOptions.newBuilder()
        .setMode(new alluxio.security.authorization.Mode((short) mode).toProto()).build();
    try {
      mFileSystem.setAttribute(uri, options);
    } catch (Throwable t) {
      LOG.error("Failed to change {} to mode {}", path, mode, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Changes the user and group ownership of an Alluxio file.
   * This operation only works when the user group translation is enabled in Alluxio-FUSE.
   *
   * @param path the path of the file
   * @param uid the uid to change to
   * @param gid the gid to change to
   * @return 0 on success, a negative value on error
   */
  @Override
  public int chown(String path, @uid_t long uid, @gid_t long gid) {
    if (!mIsUserGroupTranslation) {
      LOG.info("Cannot change the owner/group of path {}. Please set {} to be true to enable "
          + "user group translation in Alluxio-FUSE.",
          path, PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED.getName());
      return -ErrorCodes.EOPNOTSUPP();
    }

    try {
      SetAttributePOptions.Builder optionsBuilder = SetAttributePOptions.newBuilder();
      final AlluxioURI uri = mPathResolverCache.getUnchecked(path);

      String userName = "";
      if (uid != ID_NOT_SET_VALUE && uid != ID_NOT_SET_VALUE_UNSIGNED) {
        userName = AlluxioFuseUtils.getUserName(uid);
        if (userName.isEmpty()) {
          // This should never be reached
          LOG.error("Failed to get user name from uid {}", uid);
          return -ErrorCodes.EINVAL();
        }
        optionsBuilder.setOwner(userName);
      }

      String groupName = "";
      if (gid != ID_NOT_SET_VALUE && gid != ID_NOT_SET_VALUE_UNSIGNED) {
        groupName = AlluxioFuseUtils.getGroupName(gid);
        if (groupName.isEmpty()) {
          // This should never be reached
          LOG.error("Failed to get group name from gid {}", gid);
          return -ErrorCodes.EINVAL();
        }
        optionsBuilder.setGroup(groupName);
      } else if (!userName.isEmpty()) {
        groupName = AlluxioFuseUtils.getGroupName(userName);
        optionsBuilder.setGroup(groupName);
      }

      if (userName.isEmpty() && groupName.isEmpty()) {
        // This should never be reached
        LOG.info("Unable to change owner and group of file {} when uid is {} and gid is {}", path,
            userName, groupName);
      } else if (userName.isEmpty()) {
        LOG.info("Change group of file {} to {}", path, groupName);
        mFileSystem.setAttribute(uri, optionsBuilder.build());
      } else {
        LOG.info("Change owner of file {} to {}", path, groupName);
        mFileSystem.setAttribute(uri, optionsBuilder.build());
      }
    } catch (Throwable t) {
      LOG.error("Failed to chown {} to uid {} and gid {}", path, uid, gid, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }
    return 0;
  }

  /**
   * Creates and opens a new file.
   *
   * @param path The FS path of the file to open
   * @param mode mode flags
   * @param fi FileInfo data struct kept by FUSE
   * @return 0 on success. A negative value on error
   */
  @Override
  public int create(String path, @mode_t long mode, FuseFileInfo fi) {
    final AlluxioURI uri = mPathResolverCache.getUnchecked(path);
    final int flags = fi.flags.get();
    LOG.info("create({}, {}) [Alluxio: {}]", path, Integer.toHexString(flags), uri);

    if (uri.getName().length() > MAX_NAME_LENGTH) {
      LOG.error("Failed to create {}, file name is longer than {} characters",
          path, MAX_NAME_LENGTH);
      return -ErrorCodes.ENAMETOOLONG();
    }
    try {
      if (mOpenFiles.size() >= MAX_OPEN_FILES) {
        LOG.error("Cannot create {}: too many open files (MAX_OPEN_FILES: {})", path,
            MAX_OPEN_FILES);
        return -ErrorCodes.EMFILE();
      }
      SetAttributePOptions.Builder attributeOptionsBuilder = SetAttributePOptions.newBuilder();
      FuseContext fc = getContext();
      long uid = fc.uid.get();
      long gid = fc.gid.get();

      if (gid != GID) {
        String groupName = AlluxioFuseUtils.getGroupName(gid);
        if (groupName.isEmpty()) {
          // This should never be reached since input gid is always valid
          LOG.error("Failed to get group name from gid {}.", gid);
          return -ErrorCodes.EFAULT();
        }
        attributeOptionsBuilder.setGroup(groupName);
      }
      if (uid != UID) {
        String userName = AlluxioFuseUtils.getUserName(uid);
        if (userName.isEmpty()) {
          // This should never be reached since input uid is always valid
          LOG.error("Failed to get user name from uid {}", uid);
          return -ErrorCodes.EFAULT();
        }
        attributeOptionsBuilder.setOwner(userName);
      }
      SetAttributePOptions setAttributePOptions = attributeOptionsBuilder.build();
      FileOutStream os = mFileSystem.createFile(uri,
          CreateFilePOptions.newBuilder()
              .setMode(new alluxio.security.authorization.Mode((short) mode).toProto())
              .build());
      long fid = mNextOpenFileId.getAndIncrement();
      mOpenFiles.add(new OpenFileEntry(fid, path, null, os));
      fi.fh.set(fid);
      if (gid != GID || uid != UID) {
        LOG.debug("Set attributes of path {} to {}", path, setAttributePOptions);
        mFileSystem.setAttribute(uri, setAttributePOptions);
      }
      LOG.debug("{} created and opened", path);
    } catch (FileAlreadyExistsException e) {
      LOG.debug("Failed to create {}, file already exists", path);
      return -ErrorCodes.EEXIST();
    } catch (InvalidPathException e) {
      LOG.debug("Failed to create {}, path is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOG.error("Failed to create {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Flushes cached data on Alluxio.
   *
   * Called on explicit sync() operation or at close().
   *
   * @param path The path on the FS of the file to close
   * @param fi FileInfo data struct kept by FUSE
   * @return 0 on success, a negative value on error
   */
  @Override
  public int flush(String path, FuseFileInfo fi) {
    LOG.info("flush({})", path);

    return 0;
  }

  /**
   * Retrieves file attributes.
   *
   * @param path The path on the FS of the file
   * @param stat FUSE data structure to fill with file attrs
   * @return 0 on success, negative value on error
   */
  @Override
  public int getattr(String path, FileStat stat) {
    String targetPath = ramDiskDIR + path;
//    targetPath.replaceAll(mntPoint, ramDiskDIR);
    File file = new File(targetPath);

    LOG.info("getattr({})", path);
    try {
      if (file.isDirectory()) {
        stat.st_mode.set(FileStat.S_IFDIR | 0755);
      } else {
        stat.st_mode.set(FileStat.S_IFREG | 0444);
        long size = file.length();
        stat.st_size.set(size);
      }
      stat.st_nlink.set(1);
    } catch (Throwable t) {
      LOG.error("Failed to get info of {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }
    
    return 0;
  }

  /**
   * @return Name of the file system
   */
  @Override
  public String getFSName() {
    return mFsName;
  }

  /**
   * Creates a new dir.
   *
   * @param path the path on the FS of the new dir
   * @param mode Dir creation flags (IGNORED)
   * @return 0 on success, a negative value on error
   */
  @Override
  public int mkdir(String path, @mode_t long mode) {
    final AlluxioURI turi = mPathResolverCache.getUnchecked(path);
    LOG.info("mkdir({}) [Alluxio: {}]", path, turi);
    if (turi.getName().length() > MAX_NAME_LENGTH) {
      LOG.error("Failed to create directory {}, directory name is longer than {} characters",
          path, MAX_NAME_LENGTH);
      return -ErrorCodes.ENAMETOOLONG();
    }
    FuseContext fc = getContext();
    long uid = fc.uid.get();
    long gid = fc.gid.get();
    try {
      String groupName = AlluxioFuseUtils.getGroupName(gid);
      if (groupName.isEmpty()) {
        // This should never be reached since input gid is always valid
        LOG.error("Failed to get group name from gid {}.", gid);
        return -ErrorCodes.EFAULT();
      }
      String userName = AlluxioFuseUtils.getUserName(uid);
      if (userName.isEmpty()) {
        // This should never be reached since input uid is always valid
        LOG.error("Failed to get user name from uid {}", uid);
        return -ErrorCodes.EFAULT();
      }
      mFileSystem.createDirectory(turi,
          CreateDirectoryPOptions.newBuilder()
              .setMode(new alluxio.security.authorization.Mode((short) mode).toProto())
              .build());
      mFileSystem.setAttribute(turi, SetAttributePOptions.newBuilder()
          .setOwner(userName).setGroup(groupName).build());
    } catch (FileAlreadyExistsException e) {
      LOG.debug("Failed to create directory {}, directory already exists", path);
      return -ErrorCodes.EEXIST();
    } catch (InvalidPathException e) {
      LOG.debug("Failed to create directory {}, path is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOG.error("Failed to create directory {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Opens an existing file for reading.
   *
   * Note that the opening an existing file would fail, because of Alluxio's write-once semantics.
   *
   * @param path the FS path of the file to open
   * @param fi FileInfo data structure kept by FUSE
   * @return 0 on success, a negative value on error
   */
  @Override
  public int open(String path, FuseFileInfo fi) {
    LOG.info("open({})", path);
    return 0;
  }

  /**
   * Reads data from an open file.
   *
   * @param path the FS path of the file to read
   * @param buf FUSE buffer to fill with data read
   * @param size how many bytes to read. The maximum value that is accepted
   *             on this method is {@link Integer#MAX_VALUE} (note that current
   *             FUSE implementation will call this method with a size of
   *             at most 128K).
   * @param offset offset of the read operation
   * @param fi FileInfo data structure kept by FUSE
   * @return the number of bytes read or 0 on EOF. A negative
   *         value on error
   */
  @Override
  public int read(String path, Pointer buf, @size_t long size, @off_t long offset,
      FuseFileInfo fi) {
    LOG.debug("readEntry({}, {}, {})", path, size, offset);

    if (size > Integer.MAX_VALUE) {
      LOG.error("Cannot read more than Integer.MAX_VALUE");
      return -ErrorCodes.EINVAL();
    }
//    String targetPath = ramDiskDIR + path;
    int bytesToRead=-1;

    bytesToRead = (int) Math.min(139793899-offset, size);
    byte[] bytesRead = new byte[bytesToRead];
    buf.put(0, bytesRead, 0, bytesToRead);

//    if (fileContents.containsKey(targetPath)) {
//      bytesToRead = (int) Math.min(fileContents.get(targetPath) - offset, size);
//      byte[] bytesRead = new byte[bytesToRead];
//      buf.put(0, bytesRead, 0, bytesToRead);
//    }else{
//      LOG.error("Failed to find the path in cache {}", targetPath);
//    }

    return bytesToRead;
  }

  /**
   * Reads the contents of a directory.
   *
   * @param path The FS path of the directory
   * @param buff The FUSE buffer to fill
   * @param filter FUSE filter
   * @param offset Ignored in alluxio-fuse
   * @param fi FileInfo data structure kept by FUSE
   * @return 0 on success, a negative value on error
   */
  @Override
  public int readdir(String path, Pointer buff, FuseFillDir filter,
      @off_t long offset, FuseFileInfo fi) {

    LOG.info("readdir({})", path);
    String[] pathnames;
    String targetPath = ramDiskDIR + path;
    File dir = new File(targetPath);
    pathnames = dir.list();

    LOG.info("readdir({})", path);

    try {
      // standard . and .. entries
      filter.apply(buff, ".", null, 0);
      filter.apply(buff, "..", null, 0);

      for (final String filename : pathnames) {
        filter.apply(buff, filename, null, 0);
//        String fullPathName = targetPath + "/" + filename;
//        File current = new File(fullPathName);
//        if (current.isDirectory()){
//          continue;
//        }
//
//        if (!fileContents.containsKey(fullPathName) ) {
//          Path p = Paths.get(fullPathName);
//          byte[] contentBytes = Files.readAllBytes(p);
//          LOG.info("put({}) into filter.", fullPathName);
//          fileContents.put(fullPathName, ByteBuffer.wrap(contentBytes).capacity());
//        }
      }
    }  catch (Throwable t) {
      LOG.error("Failed to read directory {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Releases the resources associated to an open file. Release() is async.
   *
   * Guaranteed to be called once for each open() or create().
   *
   * @param path the FS path of the file to release
   * @param fi FileInfo data structure kept by FUSE
   * @return 0. The return value is ignored by FUSE (any error should be reported
   *         on flush instead)
   */
  @Override
  public int release(String path, FuseFileInfo fi) {
    LOG.info("release({})", path);

    return 0;
  }

  /**
   * Renames a path.
   *
   * @param oldPath the source path in the FS
   * @param newPath the destination path in the FS
   * @return 0 on success, a negative value on error
   */
  @Override
  public int rename(String oldPath, String newPath) {
    final AlluxioURI oldUri = mPathResolverCache.getUnchecked(oldPath);
    final AlluxioURI newUri = mPathResolverCache.getUnchecked(newPath);
    final String name = newUri.getName();
    LOG.info("rename({}, {}) [Alluxio: {}, {}]", oldPath, newPath, oldUri, newUri);

    if (name.length() > MAX_NAME_LENGTH) {
      LOG.error("Failed to rename {} to {}, name {} is longer than {} characters",
          oldPath, newPath, name, MAX_NAME_LENGTH);
      return -ErrorCodes.ENAMETOOLONG();
    }
    try {
      mFileSystem.rename(oldUri, newUri);
      OpenFileEntry oe = mOpenFiles.getFirstByField(PATH_INDEX, oldPath);
      if (oe != null) {
        oe.setPath(newPath);
      }
    } catch (FileDoesNotExistException e) {
      LOG.debug("Failed to rename {} to {}, file {} does not exist", oldPath, newPath, oldPath);
      return -ErrorCodes.ENOENT();
    } catch (FileAlreadyExistsException e) {
      LOG.debug("Failed to rename {} to {}, file {} already exists", oldPath, newPath, newPath);
      return -ErrorCodes.EEXIST();
    } catch (Throwable t) {
      LOG.error("Failed to rename {} to {}", oldPath, newPath, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Deletes an empty directory.
   *
   * @param path The FS path of the directory
   * @return 0 on success, a negative value on error
   */
  @Override
  public int rmdir(String path) {
    LOG.info("rmdir({})", path);
    return rmInternal(path);
  }

  /**
   * Gets the filesystem statistics.
   *
   * @param path The FS path of the directory
   * @param stbuf Statistics of a filesystem
   * @return 0 on success, a negative value on error
   */
  @Override
  public int statfs(String path, Statvfs stbuf) {
    LOG.info("statfs({})", path);
    ClientContext ctx = ClientContext.create(sConf);

    try (BlockMasterClient blockClient =
             BlockMasterClient.Factory.create(MasterClientContext.newBuilder(ctx).build())) {
      Set<BlockMasterInfo.BlockMasterInfoField> blockMasterInfoFilter =
          new HashSet<>(Arrays.asList(
              BlockMasterInfo.BlockMasterInfoField.CAPACITY_BYTES,
              BlockMasterInfo.BlockMasterInfoField.FREE_BYTES,
              BlockMasterInfo.BlockMasterInfoField.USED_BYTES));
      BlockMasterInfo blockMasterInfo = blockClient.getBlockMasterInfo(blockMasterInfoFilter);

      // although user may set different block size for different files,
      // small block size can result more accurate compute.
      long blockSize = 4 * Constants.KB;
      // fs block size
      // The size in bytes of the minimum unit of allocation on this file system
      stbuf.f_bsize.set(blockSize);
      // The preferred length of I/O requests for files on this file system.
      stbuf.f_frsize.set(blockSize);
      // total data blocks in fs
      stbuf.f_blocks.set(blockMasterInfo.getCapacityBytes() / blockSize);
      // free blocks in fs
      long freeBlocks = blockMasterInfo.getFreeBytes() / blockSize;
      stbuf.f_bfree.set(freeBlocks);
      stbuf.f_bavail.set(freeBlocks);
      // inode info in fs
      // TODO(liuhongtong): support inode info
      stbuf.f_files.set(UNKNOWN_INODES);
      stbuf.f_ffree.set(UNKNOWN_INODES);
      stbuf.f_favail.set(UNKNOWN_INODES);
      // max file name length
      stbuf.f_namemax.set(MAX_NAME_LENGTH);
    } catch (IOException e) {
      LOG.error("statfs({}) failed:", path, e);
      return -ErrorCodes.EIO();
    }
    return 0;
  }

  /**
   * Changes the size of a file. This operation would not succeed because of Alluxio's write-once
   * model.
   */
  @Override
  public int truncate(String path, long size) {
    LOG.error("Truncate is not supported {}", path);
    return -ErrorCodes.EOPNOTSUPP();
  }

  /**
   * Deletes a file from the FS.
   *
   * @param path the FS path of the file
   * @return 0 on success, a negative value on error
   */
  @Override
  public int unlink(String path) {
    LOG.info("unlink({})", path);
    return rmInternal(path);
  }

  /**
   * Alluxio does not have access time, and the file is created only once. So this operation is a
   * no-op.
   */
  @Override
  public int utimens(String path, Timespec[] timespec) {
    return 0;
  }

  /**
   * Writes a buffer to an open Alluxio file. Random write is not supported, so the offset argument
   * is ignored. Also, due to an issue in OSXFUSE that may write the same content at a offset
   * multiple times, the write also checks that the subsequent write of the same offset is ignored.
   *
   * @param buf The buffer with source data
   * @param size How much data to write from the buffer. The maximum accepted size for writes is
   *        {@link Integer#MAX_VALUE}. Note that current FUSE implementation will anyway call write
   *        with at most 128K writes
   * @param offset The offset where to write in the file (IGNORED)
   * @param fi FileInfo data structure kept by FUSE
   * @return number of bytes written on success, a negative value on error
   */
  @Override
  public int write(String path, Pointer buf, @size_t long size, @off_t long offset,
                   FuseFileInfo fi) {
    if (size > Integer.MAX_VALUE) {
      LOG.error("Cannot write more than Integer.MAX_VALUE");
      return ErrorCodes.EIO();
    }
    LOG.info("write({}, {}, {})", path, size, offset);
    final int sz = (int) size;
    final long fd = fi.fh.get();
    OpenFileEntry oe = mOpenFiles.getFirstByField(ID_INDEX, fd);
    if (oe == null) {
      LOG.error("Cannot find fd for {} in table", path);
      return -ErrorCodes.EBADFD();
    }

    if (oe.getOut() == null) {
      LOG.error("{} already exists in Alluxio and cannot be overwritten."
          + " Please delete this file first.", path);
      return -ErrorCodes.EEXIST();
    }

    if (offset < oe.getWriteOffset()) {
      // no op
      return sz;
    }

    try {
      final byte[] dest = new byte[sz];
      buf.get(0, dest, 0, sz);
      oe.getOut().write(dest);
      oe.setWriteOffset(offset + size);
    } catch (IOException e) {
      LOG.error("IOException while writing to {}.", path, e);
      return -ErrorCodes.EIO();
    }

    return sz;
  }

  /**
   * Convenience internal method to remove files or non-empty directories.
   *
   * @param path The path to remove
   * @return 0 on success, a negative value on error
   */
  private int rmInternal(String path) {
    final AlluxioURI turi = mPathResolverCache.getUnchecked(path);

    try {
      mFileSystem.delete(turi);
    } catch (FileDoesNotExistException | InvalidPathException e) {
      LOG.debug("Failed to remove {}, file does not exist or is invalid", path);
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOG.error("Failed to remove {}", path, t);
      return AlluxioFuseUtils.getErrorCode(t);
    }

    return 0;
  }

  /**
   * Waits for the file to complete before opening it.
   *
   * @param uri the file path to check
   * @return whether the file is completed or not
   */
  private boolean waitForFileCompleted(AlluxioURI uri) {
    try {
      CommonUtils.waitFor("file completed", () -> {
        try {
          return mFileSystem.getStatus(uri).isCompleted();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }, WaitForOptions.defaults().setTimeoutMs(MAX_OPEN_WAITTIME_MS));
      return true;
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException te) {
      return false;
    }
  }

  /**
   * Exposed for testing.
   */
  LoadingCache<String, AlluxioURI> getPathResolverCache() {
    return mPathResolverCache;
  }

  /**
   * Resolves a FUSE path into {@link AlluxioURI} and possibly keeps it in the cache.
   */
  private final class PathCacheLoader extends CacheLoader<String, AlluxioURI> {

    /**
     * Constructs a new {@link PathCacheLoader}.
     */
    public PathCacheLoader() {}

    @Override
    public AlluxioURI load(String fusePath) {
      // fusePath is guaranteed to always be an absolute path (i.e., starts
      // with a fwd slash) - relative to the FUSE mount point
      final String relPath = fusePath.substring(1);
      final Path tpath = mAlluxioRootPath.resolve(relPath);

      return new AlluxioURI(tpath.toString());
    }
  }
}
