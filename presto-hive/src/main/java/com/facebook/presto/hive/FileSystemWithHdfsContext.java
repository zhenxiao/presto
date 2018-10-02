/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.presto.hive.HdfsEnvironment.HdfsCallerContextSetter;
import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 * Wraps the {@link FileSystem} implementations with {@link HdfsContext}. HDFS context is set in thread local variable before calling super method and reset
 * after super method call is over.
 */
public class FileSystemWithHdfsContext
        extends FilterFileSystem
{
    private final HdfsContext hdfsContext;

    public FileSystemWithHdfsContext(final FileSystem delegate, final HdfsContext hdfsContext)
    {
        super(delegate);
        this.hdfsContext = hdfsContext;
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getFileBlockLocations(file, start, len);
        }
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path p, long start, long len)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getFileBlockLocations(p, start, len);
        }
    }

    @Override
    public FSDataInputStream open(Path path, int i)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.open(path, i);
        }
    }

    @Override
    public FSDataInputStream open(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.open(f);
        }
    }

    @Override
    public FSDataOutputStream create(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.create(f);
        }
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.create(f, overwrite);
        }
    }

    @Override
    public FSDataOutputStream create(Path f, Progressable progress)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.create(f, progress);
        }
    }

    @Override
    public FSDataOutputStream create(Path f, short replication)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.create(f, replication);
        }
    }

    @Override
    public FSDataOutputStream create(Path f, short replication, Progressable progress)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.create(f, replication, progress);
        }
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.create(f, overwrite, bufferSize);
        }
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.create(f, overwrite, bufferSize, progress);
        }
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.create(f, overwrite, bufferSize, replication, blockSize);
        }
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.create(f, overwrite, bufferSize, replication, blockSize, progress);
        }
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.create(path, fsPermission, b, i, i1, l, progressable);
        }
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.create(f, permission, flags, bufferSize, replication, blockSize, progress);
        }
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.create(f, permission, flags, bufferSize, replication, blockSize, progress, checksumOpt);
        }
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.createNonRecursive(f, overwrite, bufferSize, replication, blockSize, progress);
        }
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.createNonRecursive(f, permission, overwrite, bufferSize, replication, blockSize, progress);
        }
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize, progress);
        }
    }

    @Override
    public boolean createNewFile(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.createNewFile(f);
        }
    }

    @Override
    public FSDataOutputStream append(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.append(f);
        }
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.append(f, bufferSize);
        }
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.append(path, i, progressable);
        }
    }

    @Override
    public void concat(Path trg, Path[] psrcs)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.concat(trg, psrcs);
        }
    }

    @Override
    @Deprecated
    public short getReplication(Path src)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getReplication(src);
        }
    }

    @Override
    public boolean setReplication(Path src, short replication)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.setReplication(src, replication);
        }
    }

    @Override
    public boolean rename(Path path, Path path1)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.rename(path, path1);
        }
    }

    @Override
    public boolean truncate(Path f, long newLength)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.truncate(f, newLength);
        }
    }

    @Override
    @Deprecated
    public boolean delete(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.delete(f);
        }
    }

    @Override
    public boolean delete(Path path, boolean b)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.delete(path, b);
        }
    }

    @Override
    public boolean deleteOnExit(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.deleteOnExit(f);
        }
    }

    @Override
    public boolean cancelDeleteOnExit(Path f)
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.cancelDeleteOnExit(f);
        }
    }

    @Override
    public boolean exists(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.exists(f);
        }
    }

    @Override
    public boolean isDirectory(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.isDirectory(f);
        }
    }

    @Override
    public boolean isFile(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.isFile(f);
        }
    }

    @Override
    @Deprecated
    public long getLength(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getLength(f);
        }
    }

    @Override
    public ContentSummary getContentSummary(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getContentSummary(f);
        }
    }

    @Override
    public QuotaUsage getQuotaUsage(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getQuotaUsage(f);
        }
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws FileNotFoundException, IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.listStatus(path);
        }
    }

    @Override
    public RemoteIterator<Path> listCorruptFileBlocks(Path path)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.listCorruptFileBlocks(path);
        }
    }

    @Override
    public FileStatus[] listStatus(Path f, PathFilter filter)
            throws FileNotFoundException, IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.listStatus(f, filter);
        }
    }

    @Override
    public FileStatus[] listStatus(Path[] files)
            throws FileNotFoundException, IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.listStatus(files);
        }
    }

    @Override
    public FileStatus[] listStatus(Path[] files, PathFilter filter)
            throws FileNotFoundException, IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.listStatus(files, filter);
        }
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.globStatus(pathPattern);
        }
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.globStatus(pathPattern, filter);
        }
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
            throws FileNotFoundException, IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.listLocatedStatus(f);
        }
    }

    @Override
    public RemoteIterator<FileStatus> listStatusIterator(Path p)
            throws FileNotFoundException, IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.listStatusIterator(p);
        }
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive)
            throws FileNotFoundException, IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.listFiles(f, recursive);
        }
    }

    @Override
    public Path getHomeDirectory()
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getHomeDirectory();
        }
    }

    @Override
    public Path getWorkingDirectory()
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getWorkingDirectory();
        }
    }

    @Override
    public void setWorkingDirectory(Path path)
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.setWorkingDirectory(path);
        }
    }

    @Override
    public boolean mkdirs(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.mkdirs(f);
        }
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.mkdirs(path, fsPermission);
        }
    }

    @Override
    public void copyFromLocalFile(Path src, Path dst)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.copyFromLocalFile(src, dst);
        }
    }

    @Override
    public void moveFromLocalFile(Path[] srcs, Path dst)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.moveFromLocalFile(srcs, dst);
        }
    }

    @Override
    public void moveFromLocalFile(Path src, Path dst)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.moveFromLocalFile(src, dst);
        }
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.copyFromLocalFile(delSrc, src, dst);
        }
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.copyFromLocalFile(delSrc, overwrite, srcs, dst);
        }
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.copyFromLocalFile(delSrc, overwrite, src, dst);
        }
    }

    @Override
    public void copyToLocalFile(Path src, Path dst)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.copyToLocalFile(src, dst);
        }
    }

    @Override
    public void moveToLocalFile(Path src, Path dst)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.moveToLocalFile(src, dst);
        }
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.copyToLocalFile(delSrc, src, dst);
        }
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
        }
    }

    @Override
    public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.startLocalOutput(fsOutputFile, tmpLocalFile);
        }
    }

    @Override
    public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.completeLocalOutput(fsOutputFile, tmpLocalFile);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.close();
        }
    }

    @Override
    public long getUsed()
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getUsed();
        }
    }

    @Override
    public long getUsed(Path path)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getUsed(path);
        }
    }

    @Override
    @Deprecated
    public long getBlockSize(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getBlockSize(f);
        }
    }

    @Override
    @Deprecated
    public long getDefaultBlockSize()
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getDefaultBlockSize();
        }
    }

    @Override
    public long getDefaultBlockSize(Path f)
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getDefaultBlockSize(f);
        }
    }

    @Override
    @Deprecated
    public short getDefaultReplication()
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getDefaultReplication();
        }
    }

    @Override
    public short getDefaultReplication(Path path)
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getDefaultReplication(path);
        }
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getFileStatus(path);
        }
    }

    @Override
    @InterfaceAudience.LimitedPrivate({"HDFS", "Hive"})
    public void access(Path path, FsAction mode)
            throws AccessControlException, FileNotFoundException, IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.access(path, mode);
        }
    }

    @Override
    public void createSymlink(Path target, Path link, boolean createParent)
            throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.createSymlink(target, link, createParent);
        }
    }

    @Override
    public FileStatus getFileLinkStatus(Path f)
            throws AccessControlException, FileNotFoundException, UnsupportedFileSystemException, IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getFileLinkStatus(f);
        }
    }

    @Override
    public boolean supportsSymlinks()
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.supportsSymlinks();
        }
    }

    @Override
    public Path getLinkTarget(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getLinkTarget(f);
        }
    }

    @Override
    public FileChecksum getFileChecksum(Path f)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getFileChecksum(f);
        }
    }

    @Override
    public FileChecksum getFileChecksum(Path f, long length)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getFileChecksum(f, length);
        }
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum)
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.setVerifyChecksum(verifyChecksum);
        }
    }

    @Override
    public void setWriteChecksum(boolean writeChecksum)
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.setWriteChecksum(writeChecksum);
        }
    }

    @Override
    public FsStatus getStatus()
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getStatus();
        }
    }

    @Override
    public FsStatus getStatus(Path p)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getStatus(p);
        }
    }

    @Override
    public void setPermission(Path p, FsPermission permission)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.setPermission(p, permission);
        }
    }

    @Override
    public void setOwner(Path p, String username, String groupname)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.setOwner(p, username, groupname);
        }
    }

    @Override
    public void setTimes(Path p, long mtime, long atime)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.setTimes(p, mtime, atime);
        }
    }

    @Override
    public Path createSnapshot(Path path, String snapshotName)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.createSnapshot(path, snapshotName);
        }
    }

    @Override
    public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.renameSnapshot(path, snapshotOldName, snapshotNewName);
        }
    }

    @Override
    public void deleteSnapshot(Path path, String snapshotName)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.deleteSnapshot(path, snapshotName);
        }
    }

    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.modifyAclEntries(path, aclSpec);
        }
    }

    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclSpec)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.removeAclEntries(path, aclSpec);
        }
    }

    @Override
    public void removeDefaultAcl(Path path)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.removeDefaultAcl(path);
        }
    }

    @Override
    public void removeAcl(Path path)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.removeAcl(path);
        }
    }

    @Override
    public void setAcl(Path path, List<AclEntry> aclSpec)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.setAcl(path, aclSpec);
        }
    }

    @Override
    public AclStatus getAclStatus(Path path)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getAclStatus(path);
        }
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.setXAttr(path, name, value);
        }
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.setXAttr(path, name, value, flag);
        }
    }

    @Override
    public byte[] getXAttr(Path path, String name)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getXAttr(path, name);
        }
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getXAttrs(path);
        }
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path, List<String> names)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getXAttrs(path, names);
        }
    }

    @Override
    public List<String> listXAttrs(Path path)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.listXAttrs(path);
        }
    }

    @Override
    public void removeXAttr(Path path, String name)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.removeXAttr(path, name);
        }
    }

    @Override
    public void setStoragePolicy(Path src, String policyName)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.setStoragePolicy(src, policyName);
        }
    }

    @Override
    public void unsetStoragePolicy(Path src)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            super.unsetStoragePolicy(src);
        }
    }

    @Override
    public BlockStoragePolicySpi getStoragePolicy(Path src)
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getStoragePolicy(src);
        }
    }

    @Override
    public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
            throws IOException
    {
        try (HdfsCallerContextSetter ignored = withContextSetter()) {
            return super.getAllStoragePolicies();
        }
    }

    private HdfsCallerContextSetter withContextSetter()
    {
        return new HdfsCallerContextSetter(hdfsContext);
    }
}
