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
package com.facebook.presto.storage.hdfs;

import com.facebook.presto.storage.hdfs.blockcache.CustomBufferedIndexInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class HdfsDirectory extends BaseDirectory {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final int BUFFER_SIZE = 65536;

    public static final int NUM_BUFFER_SIZE = 512;
    public int currentNumBufferSize = 0;
    public boolean update = false;

    private final AtomicLong nextTempFileCounter = new AtomicLong();
    private final Map<Path, Path> copyLinks = new HashMap<>();

    private static final String LF_EXT = ".lf";
    protected final Path hdfsDirPath;
    protected final Configuration configuration;

    private final FileSystem fileSystem;
    private final FileContext fileContext;
    private boolean isOpenForReader;
    private String key;
    private LinkedBlockingQueue<UpdateTaskData> updateQueue;
    private Map<String, FileStatus> fileStatusCache;
    private AtomicLong sizeInBytes = new AtomicLong();
    public final AtomicLong sizeUsed = new AtomicLong();

    public HdfsDirectory(String key, Path hdfsDirPath, Configuration configuration, FileSystem fileSystem)
            throws IOException {
        this(key, hdfsDirPath, configuration, fileSystem, false);
    }

    public HdfsDirectory(String key, Path hdfsDirPath, Configuration configuration, FileSystem fileSystem,
            boolean isOpenForReader) throws IOException {
        this(key, hdfsDirPath, HdfsLockFactory.INSTANCE, configuration, fileSystem, isOpenForReader, null);
    }

    public HdfsDirectory(String key, Path hdfsDirPath, Configuration configuration, FileSystem fileSystem,
            boolean isOpenForReader, LinkedBlockingQueue<UpdateTaskData> updateQueue) throws IOException {
        this(key, hdfsDirPath, HdfsLockFactory.INSTANCE, configuration, fileSystem, isOpenForReader, updateQueue);
    }

    public HdfsDirectory(String key, Path hdfsDirPath, LockFactory lockFactory, Configuration configuration,
            FileSystem fileSystem, boolean isOpenForReader, LinkedBlockingQueue<UpdateTaskData> updateQueue)
            throws IOException {
        super(lockFactory);
        this.hdfsDirPath = hdfsDirPath;
        this.configuration = configuration;
        this.fileSystem = fileSystem;
        this.isOpenForReader = isOpenForReader;
        this.updateQueue = updateQueue;
        this.key = key;
        if (isOpenForReader) {
            fileStatusCache = new ConcurrentHashMap<>();
        }
        fileContext = FileContext.getFileContext(hdfsDirPath.toUri(), configuration);

        try {
            if (!fileSystem.exists(hdfsDirPath)) {
                boolean success = fileSystem.mkdirs(hdfsDirPath);
                if (!success) {
                    throw new RuntimeException("Could not create directory: " + hdfsDirPath);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Problem creating directory: " + hdfsDirPath, e);
        }
    }

    public void notifyUpdate() {
        if (update) {
            long currentWeight = currentNumBufferSize * NUM_BUFFER_SIZE * BUFFER_SIZE;
            if (sizeUsed.get() > currentWeight && currentWeight < sizeInBytes.get()) {
                synchronized (this) {
                    if (sizeUsed.get() > currentWeight && currentWeight < sizeInBytes.get()) {
                        currentNumBufferSize++;
                        long afterWeight = 0;
                        long blocksSize = currentNumBufferSize * NUM_BUFFER_SIZE * BUFFER_SIZE;
                        if ((blocksSize) < sizeInBytes.get()) {
                            afterWeight = blocksSize;
                        } else {
                            afterWeight = sizeInBytes.get();
                        }
                        try {
                            updateQueue.put(new UpdateTaskData(key, afterWeight));
                        } catch (InterruptedException e) {
                            LOG.warn("error when put update task", e);
                        }
                    }
                }
            }
        }
    }

    public void startUpdate() {
        this.update = true;
    }

    public void stopUpdate() {
        this.update = false;
    }

    public boolean isOpenForReader() {
        return isOpenForReader;
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing hdfs directory {}", hdfsDirPath);
        isOpen = false;
    }

    /**
     * Check whether this directory is open or closed. This check may return
     * stale results in the form of false negatives.
     * 
     * @return true if the directory is definitely closed, false if the
     *         directory is open or is pending closure
     */
    public boolean isClosed() {
        return !isOpen;
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return new HdfsFileWriter(getFileSystem(), new Path(hdfsDirPath, name));
    }

    private String[] getNormalNames(List<String> files) {
        int size = files.size();
        for (int i = 0; i < size; i++) {
            String str = files.get(i);
            files.set(i, toNormalName(str));
        }
        return files.toArray(new String[] {});
    }

    private String toNormalName(String name) {
        if (name.endsWith(LF_EXT)) {
            return name.substring(0, name.length() - 3);
        }
        return name;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return openInput(name);
    }

    private IndexInput openInput(String name) throws IOException {
        if (copyLinks.get(new Path(hdfsDirPath, name)) != null) {
            Path key = new Path(hdfsDirPath, name);
            if (isOpenForReader && fileStatusCache.get(key.toString()) != null) {
                sizeInBytes.addAndGet(computeMemUsed(fileStatusCache.get(key.toString()).getLen()));
                return new HdfsIndexInput(name, getFileSystem(), copyLinks.get(key),
                        (int) Math.min(fileStatusCache.get(key.toString()).getLen(), BUFFER_SIZE),
                        fileStatusCache.get(key.toString()), isOpenForReader, this);
            } else if (isOpenForReader && fileStatusCache.get(key.toString()) == null) {
                FileStatus file = getFileSystem().getFileStatus(copyLinks.get(key));
                fileStatusCache.put(key.toString(), file);
                sizeInBytes.addAndGet(computeMemUsed(file.getLen()));
                return new HdfsIndexInput(name, getFileSystem(), copyLinks.get(key),
                        (int) Math.min(file.getLen(), BUFFER_SIZE), file, isOpenForReader, this);
            } else {
                return new HdfsIndexInput(name, getFileSystem(), copyLinks.get(key), BUFFER_SIZE, null, isOpenForReader,
                        this);
            }
        } else {
            if (isOpenForReader && fileStatusCache.get(name) != null) {
                sizeInBytes.addAndGet(computeMemUsed(fileStatusCache.get(name).getLen()));
                return new HdfsIndexInput(name, getFileSystem(), new Path(hdfsDirPath, name),
                        (int) Math.min(fileStatusCache.get(name).getLen(), BUFFER_SIZE), fileStatusCache.get(name),
                        isOpenForReader, this);
            } else if (isOpenForReader && fileStatusCache.get(name) == null) {
                FileStatus file = getFileSystem().getFileStatus(new Path(hdfsDirPath, name));
                fileStatusCache.put(name, file);
                sizeInBytes.addAndGet(computeMemUsed(file.getLen()));
                return new HdfsIndexInput(name, getFileSystem(), new Path(hdfsDirPath, name),
                        (int) Math.min(file.getLen(), BUFFER_SIZE), file, isOpenForReader, this);
            } else {
                return new HdfsIndexInput(name, getFileSystem(), new Path(hdfsDirPath, name), BUFFER_SIZE, null,
                        isOpenForReader, this);
            }
        }
    }

    private long computeMemUsed(long size) {
        if (size < BUFFER_SIZE) {
            return size;
        } else {
            if ((size % BUFFER_SIZE) == 0) {
                return size;
            } else {
                return ((size / BUFFER_SIZE) + 1) * BUFFER_SIZE;
            }
        }
    }

    @Override
    public void deleteFile(String name) throws IOException {
        Path path = new Path(hdfsDirPath, name);
        if (copyLinks.get(path) != null) {
            return;
        }
        LOG.debug("Deleting {}", path);
        getFileSystem().delete(path, true);
    }

    @Override
    public void renameFile(String source, String dest) throws IOException {
        Path sourcePath = new Path(hdfsDirPath, source);
        if (copyLinks.get(sourcePath) != null) {
            return;
        }
        Path destPath = new Path(hdfsDirPath, dest);
        fileContext.rename(sourcePath, destPath);
    }

    @Override
    public long fileLength(String name) throws IOException {
        if (copyLinks.get(new Path(hdfsDirPath, name)) != null) {
            Path key = new Path(hdfsDirPath, name);
            if (isOpenForReader && fileStatusCache.get(key.toString()) != null) {
                return fileStatusCache.get(key.toString()).getLen();
            } else if (isOpenForReader && fileStatusCache.get(key.toString()) == null) {
                FileStatus file = getFileSystem().getFileStatus(copyLinks.get(key));
                fileStatusCache.put(key.toString(), file);
                return file.getLen();
            }
            return getFileSystem().getFileStatus(copyLinks.get(key)).getLen();
        } else {
            if (isOpenForReader && fileStatusCache.get(name) != null) {
                return fileStatusCache.get(name).getLen();
            } else if (isOpenForReader && fileStatusCache.get(name) == null) {
                FileStatus file = getFileSystem().getFileStatus(new Path(hdfsDirPath, name));
                fileStatusCache.put(name, file);
                return file.getLen();
            }
            return getFileSystem().getFileStatus(new Path(hdfsDirPath, name)).getLen();
        }
    }

    public long fileModified(String name) throws IOException {
        if (copyLinks.get(new Path(hdfsDirPath, name)) != null) {
            Path key = new Path(hdfsDirPath, name);
            if (isOpenForReader && fileStatusCache.get(key.toString()) != null) {
                return fileStatusCache.get(key.toString()).getModificationTime();
            } else if (isOpenForReader && fileStatusCache.get(key.toString()) == null) {
                FileStatus file = getFileSystem().getFileStatus(copyLinks.get(key));
                fileStatusCache.put(key.toString(), file);
                return file.getModificationTime();
            }
            FileStatus fileStatus = getFileSystem().getFileStatus(copyLinks.get(key));
            return fileStatus.getModificationTime();
        } else {
            if (isOpenForReader && fileStatusCache.get(name) != null) {
                return fileStatusCache.get(name).getModificationTime();
            } else if (isOpenForReader && fileStatusCache.get(name) == null) {
                FileStatus file = getFileSystem().getFileStatus(new Path(hdfsDirPath, name));
                fileStatusCache.put(name, file);
                return file.getModificationTime();
            }
            FileStatus fileStatus = getFileSystem().getFileStatus(new Path(hdfsDirPath, name));
            return fileStatus.getModificationTime();
        }
    }

    @Override
    public String[] listAll() throws IOException {
        FileStatus[] listStatus = getFileSystem().listStatus(hdfsDirPath);
        List<String> files = new ArrayList<String>();
        if (listStatus == null) {
            return new String[] {};
        }
        for (FileStatus status : listStatus) {
            files.add(status.getPath().getName());
            if (isOpenForReader) {
                fileStatusCache.put(status.getPath().getName(), status);
            }
        }
        return getNormalNames(files);
    }

    @Override
    public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        if (from instanceof HdfsDirectory) {
            HdfsDirectory d = (HdfsDirectory) from;
            Path des = new Path(this.getHdfsDirPath(), dest);
            Path s = new Path(d.getHdfsDirPath(), src);
            copyLinks.put(des, s);
        } else {
            super.copyFrom(from, src, dest, context);
        }
    }

    public Path getHdfsDirPath() {
        return hdfsDirPath;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    static class HdfsIndexInput extends CustomBufferedIndexInput {
        private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        private final Path path;
        private final FSDataInputStream inputStream;
        private final long length;
        private final HdfsDirectory directory;

        public HdfsIndexInput(String name, FileSystem fileSystem, Path path, int bufferSize, FileStatus file,
                boolean isOpenForReader, HdfsDirectory directory) throws IOException {
            super(name, bufferSize);
            this.directory = directory;
            this.path = path;
            LOG.debug("Opening normal index input on {}", path);
            FileStatus fileStatus = null;
            if (isOpenForReader) {
                fileStatus = file;
            } else {
                fileStatus = fileSystem.getFileStatus(path);
            }
            length = fileStatus.getLen();
            super.init(length);
            inputStream = fileSystem.open(path, HdfsDirectory.BUFFER_SIZE);
        }

        protected void readInternal(long filePointer, byte[] b, int offset, int length) throws IOException {
            inputStream.readFully(filePointer, b, offset, length);
            directory.sizeUsed.getAndAdd(this.length < BUFFER_SIZE ? length : HdfsDirectory.BUFFER_SIZE);
            directory.notifyUpdate();
        }

        @Override
        protected void seekInternal(long pos) throws IOException {

        }

        @Override
        protected void closeInternal() throws IOException {
            LOG.debug("Closing normal index input on {}", path);
            if (!isClone()) {
                inputStream.close();
            }
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public IndexInput clone() {
            HdfsIndexInput clone = (HdfsIndexInput) super.clone();
            return clone;
        }
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        LOG.debug("Sync called on {}", Arrays.toString(names.toArray()));
    }

    @Override
    public int hashCode() {
        return hdfsDirPath.hashCode();
    }

    public final long ramBytesUsed() {
        if (isOpenForReader) {
            ensureOpen();
            return sizeUsed.get();
        } else {
            return 0;
        }
    }
    
    public final long totalBytes() {
        if (isOpenForReader) {
            ensureOpen();
            return sizeInBytes.get();
        } else {
            return 0;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof HdfsDirectory)) {
            return false;
        }
        return this.hdfsDirPath.equals(((HdfsDirectory) obj).hdfsDirPath);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "@" + hdfsDirPath + " lockFactory=" + lockFactory;
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        String name = IndexFileNames.segmentFileName(prefix,
                suffix + "_" + Long.toString(nextTempFileCounter.getAndIncrement(), Character.MAX_RADIX), "tmp");
        return new HdfsFileWriter(getFileSystem(), new Path(hdfsDirPath, name));
    }
}
