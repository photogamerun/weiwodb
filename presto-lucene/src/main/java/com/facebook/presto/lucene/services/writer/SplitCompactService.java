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
package com.facebook.presto.lucene.services.writer;

import static java.util.Objects.requireNonNull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.weakref.jmx.internal.guava.base.Joiner;

import com.alibaba.fastjson.JSON;
import com.facebook.presto.lucene.LuceneConfig;
import com.facebook.presto.lucene.WeiwoDBHdfsConfiguration;
import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;
import com.facebook.presto.lucene.hdfs.HdfsDirectory;
import com.facebook.presto.lucene.services.writer.WeiwoDBSplitWriterManager.WriterKey;
import com.facebook.presto.lucene.services.writer.util.IKAnalyzerUtil;
import com.facebook.presto.lucene.services.writer.util.IndexNameGenarator;

import io.airlift.log.Logger;

public class SplitCompactService {

    private static final Logger log = Logger.get(SplitCompactService.class);

    public long deleteDelay = 3600 * 1000;
    public long checkInterval = 60 * 1000;
    private List<WriterKey> compacts;
    private Map<WriterKey, Compaction> compacting;
    private ExecutorService compactService;
    ReentrantReadWriteLock lock;
    WriteLock writeLock;
    ReadLock readLock;
    Configuration conf;
    FileSystem fs;
    LuceneConfig config;
    private List<TimeIndex> shouldDeletes;

    @Inject
    public SplitCompactService(LuceneConfig config, WeiwoDBHdfsConfiguration wconfig) throws IOException {
        this.compacts = new ArrayList<>();
        this.compacting = new ConcurrentHashMap<>();
        this.compactService = Executors.newFixedThreadPool(4);
        this.lock = new ReentrantReadWriteLock(true);
        this.writeLock = this.lock.writeLock();
        this.readLock = this.lock.readLock();
        this.conf = wconfig.getConfiguration();
        this.fs = FileSystem.get(conf);
        this.config = config;
        this.shouldDeletes = new ArrayList<>();
        Timer timer = new Timer("CheckService Thread", true);
        timer.schedule(new CheckService(), checkInterval, checkInterval);
    }

    public void checkCompacts() throws IOException {
        log.info("Check Compacts...");
        List<WriterKey> cs = new ArrayList<>();
        readLock.lock();
        try {
            cs.addAll(compacts);
        } finally {
            readLock.unlock();
        }
        for (WriterKey key : cs) {
            if (compacting.containsKey(key)) {
                continue;
            }
            List<String> splits = getSplits(key);
            if (splits.size() >= 2) {
                long wused = 0;
                int compactNum = 0;
                List<String> wc = new ArrayList<>();
                for (String split : splits) {
                    long used = getUsed(key, split);
                    if (used <= WeiwoDBConfigureKeys.RAM_BUFFER_SIZE_MB * 1024 * 1024 * 0.5) {
                        wused = wused + used;
                        wc.add(split);
                        compactNum++;
                    }
                    if (wused > WeiwoDBConfigureKeys.RAM_BUFFER_SIZE_MB * 1024 * 1024 * 0.5) {
                        break;
                    }
                    // if(compactNum >= 2){
                    // break;
                    // }
                }
                if (wc.size() > 1) {
                    Compaction compaction = new Compaction(wc, IndexNameGenarator.genarateIndexName());
                    WeiwoDBSplitsLinkOperator slo = new WeiwoDBSplitsLinkOperator(fs,
                            config.getDataPath() + WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR + key.getDb()
                                    + WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR + key.getTable()
                                    + WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR + key.getPartition()
                                    + WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR + key.getId()
                                    + WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR + WeiwoDBConfigureKeys.WEIWO_SPLITS_FILE,
                            key);
                    CompactionTask task = new CompactionTask(slo, compaction, config, key);
                    compactService.execute(task);
                    compacting.put(key, compaction);
                } else {
                    deleteCompactSplits(key);
                }
            } else {
                deleteCompactSplits(key);
            }
        }
    }

    public void checkDeletes() throws IOException {
        log.info("Check Deletes...");
        List<TimeIndex> result = new ArrayList<>();
        synchronized (shouldDeletes) {
            TimeIndex ti = new TimeIndex(System.currentTimeMillis() - deleteDelay, null, null);
            int index = Collections.binarySearch(shouldDeletes, ti);
            if (index >= 0) {
                for (int i = index; i < shouldDeletes.size(); i++) {
                    result.add(shouldDeletes.get(i));
                }
            } else {
                for (int i = -index - 1; i < shouldDeletes.size(); i++) {
                    result.add(shouldDeletes.get(i));
                }
            }
        }
        for (TimeIndex ti : result) {
            deleteHdfs(ti);
        }
    }

    public void addDelete(List<TimeIndex> tis) {
        log.info("Add deletes : " + JSON.toJSONString(tis));
        synchronized (shouldDeletes) {
            for (TimeIndex ti : tis) {
                int index = Collections.binarySearch(shouldDeletes, ti);
                if (index >= 0) {
                    shouldDeletes.add(index + 1, ti);
                } else {
                    shouldDeletes.add(-index - 1, ti);
                }
            }
        }
    }

    public void deleteHdfs(TimeIndex ti) throws IOException {
        Path delete = new Path(new Path(config.getDataPath()),
                Joiner.on(WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR).join(ti.getKey().getDb(), ti.getKey().getTable(),
                        ti.getKey().getPartition(), ti.getKey().getId(), ti.getIndexName()));
        fs.delete(delete, true);
        synchronized (shouldDeletes) {
            shouldDeletes.remove(ti);
        }
    }

    private List<String> getSplits(WriterKey key) throws IOException {
        String db = requireNonNull(key.getDb(), "db is null");
        String table = requireNonNull(key.getTable(), "table is null");
        String partition = requireNonNull(key.getPartition(), "partition is null");
        String id = requireNonNull(key.getId(), "id is null");
        String p = Joiner.on("/").join(db, table, partition, id, WeiwoDBConfigureKeys.WEIWO_SPLITS_FILE);
        Path path = new Path(config.getDataPath(), p);
        List<String> result = new ArrayList<>();
        if (fs.exists(path)) {
            try (FSDataInputStream in = fs.open(path, 2048)) {
                String split = null;
                while ((split = in.readLine()) != null) {
                    result.add(split);
                }
            }
        }
        return result;
    }

    private long getUsed(WriterKey key, String index) throws FileNotFoundException, IOException {
        String db = requireNonNull(key.getDb(), "db is null");
        String table = requireNonNull(key.getTable(), "table is null");
        String partition = requireNonNull(key.getPartition(), "partition is null");
        String id = requireNonNull(key.getId(), "id is null");
        String p = Joiner.on("/").join(db, table, partition, id, index);
        Path path = new Path(config.getDataPath(), p);
        FileStatus[] files = fs.listStatus(path);
        long used = 0;
        if (files != null) {
            for (FileStatus file : files) {
                if (!file.getPath().getName().startsWith("segments")) {
                    used = used + file.getLen();
                }
            }
        }
        return used;
    }

    void addCompactSplits(String db, String table, String partition, String id) {
        WriterKey key = new WriterKey(db, table, partition, id);
        log.info("Add compact for : " + JSON.toJSONString(key));
        readLock.lock();
        try {
            if (compacts.contains(key)) {
                return;
            }
        } finally {
            readLock.unlock();
        }
        writeLock.lock();
        try {
            if (compacts.contains(key)) {
                return;
            }
            compacts.add(key);
        } finally {
            writeLock.unlock();
        }
    }

    void deleteCompactSplits(String db, String table, String partition, String id) {
        WriterKey key = new WriterKey(db, table, partition, id);
        writeLock.lock();
        try {
            compacts.remove(key);
        } finally {
            writeLock.unlock();
        }
    }

    void deleteCompactSplits(WriterKey key) {
        writeLock.lock();
        try {
            compacts.remove(key);
        } finally {
            writeLock.unlock();
        }
    }

    class CompactionTask implements Runnable {

        WeiwoDBSplitsLinkOperator slo;
        Compaction compaction;
        LuceneConfig config;
        WriterKey key;

        public CompactionTask(WeiwoDBSplitsLinkOperator slo, Compaction compaction, LuceneConfig config,
                WriterKey key) {
            this.slo = requireNonNull(slo, "slo is null");
            this.compaction = requireNonNull(compaction, "compaction is null");
            this.config = requireNonNull(config, "config is null");
            this.key = requireNonNull(key, "key is null");
        }

        @Override
        public void run() {
            log.info("Start compact for : " + JSON.toJSONString(compaction));
            List<HdfsDirectory> ds = new ArrayList<>();
            try {
                Path compactPath = new Path(new Path(config.getDataPath()),
                        Joiner.on(WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR).join(key.getDb(), key.getTable(),
                                key.getPartition(), key.getId(), compaction.getEnd()));
                HdfsDirectory directory = null;
                IndexWriter writer = null;
                try {
                    directory = new HdfsDirectory(null, compactPath, conf, fs);
                    IndexWriterConfig luceneConfig = createConfig();
                    writer = new IndexWriter(directory, luceneConfig);
                } catch (Exception e) {
                    log.error(e, "error when create writer");
                    if (writer != null) {
                        try {
                            writer.close();
                        } catch (IOException e1) {
                        }
                    }
                    if (directory != null) {
                        try {
                            directory.close();
                        } catch (IOException e1) {
                        }
                        try {
                            directory.deleteFile(compactPath.toString());
                        } catch (IOException e1) {
                        }
                    }
                }

                boolean error = false;
                List<SegmentCommitInfo> list = new ArrayList<>();

                if (writer != null) {
                    for (String str : compaction.getSplits()) {
                        Path split = new Path(new Path(config.getDataPath()),
                                Joiner.on(WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR).join(key.getDb(), key.getTable(),
                                        key.getPartition(), key.getId(), str));
                        HdfsDirectory d = null;
                        try {
                            d = new HdfsDirectory(null, split, conf, fs);
                            ds.add(d);
                        } catch (IOException e) {
                            error = true;
                            log.error(e, "error open directory for index = " + str);
                            break;
                        }
                        try {
                            list.addAll(SegmentInfos.readLatestCommit(d).asList());
                        } catch (IOException e) {
                            error = true;
                            log.error(e, "error read segmentinfos for index = " + str);
                            break;
                        }
                    }
                } else {
                    error = true;
                }

                if (error) {
                    if (writer != null) {
                        try {
                            writer.close();
                        } catch (IOException e1) {
                        }
                    }
                    if (directory != null) {
                        try {
                            directory.close();
                        } catch (IOException e1) {
                        }
                        try {
                            directory.deleteFile(compactPath.toString());
                        } catch (IOException e1) {
                        }
                    }
                } else {
                    boolean success = false;
                    try {
                        writer.merge(new OneMerge(list));
                        writer.close();
                        directory.close();
                        success = true;
                    } catch (IOException e) {
                        log.error(e, "error when writer merge or close");
                        if (writer != null) {
                            try {
                                writer.close();
                            } catch (IOException e1) {
                            }
                        }
                        if (directory != null) {
                            try {
                                directory.close();
                            } catch (IOException e1) {
                            }
                            try {
                                directory.deleteFile(compactPath.toString());
                            } catch (IOException e1) {
                            }
                        }
                    }
                    if (success) {
                        try {
                            slo.deleteAndAddSplit(compaction.getSplits(), compaction.getEnd());
                            List<TimeIndex> tis = new ArrayList<>();
                            for (String str : compaction.getSplits()) {
                                tis.add(new TimeIndex(System.currentTimeMillis(), str, key));
                            }
                            addDelete(tis);
                        } catch (IOException e) {
                            log.error(e, "error when deleteAndAddSplit");
                            if (directory != null) {
                                try {
                                    directory.deleteFile(compactPath.toString());
                                } catch (IOException e1) {
                                }
                            }
                        }
                    }

                }
            } finally {
                compacting.remove(key);
                for (HdfsDirectory d : ds) {
                    try {
                        d.close();
                    } catch (IOException e) {
                        log.warn(e, "Close directory error");
                    }
                }
            }

        }

        private IndexWriterConfig createConfig() {
            IndexWriterConfig luceneConfig = new IndexWriterConfig(IKAnalyzerUtil.getAnalyzer());
            LogDocMergePolicy policy = new LogDocMergePolicy();
            policy.setMinMergeDocs(1);
            policy.setMergeFactor(2);
            policy.setNoCFSRatio(1.0);
            luceneConfig.setMergePolicy(policy);
            luceneConfig.setUseCompoundFile(true);
            luceneConfig.setRAMBufferSizeMB(WeiwoDBConfigureKeys.RAM_BUFFER_SIZE_MB);
            luceneConfig.setMaxBufferedDocs(WeiwoDBConfigureKeys.MAX_BUFFER_DOCS);
            return luceneConfig;
        }

    }

    static class TimeIndex implements Comparable<TimeIndex> {
        private long checkTime;
        private String indexName;
        private WriterKey key;

        public TimeIndex(long checkTime, String indexName, WriterKey key) {
            this.checkTime = checkTime;
            this.indexName = indexName;
            this.key = key;
        }

        @Override
        public int compareTo(TimeIndex o) {
            return Long.compare(o.getCheckTime(), checkTime);
        }

        public String getIndexName() {
            return indexName;
        }

        public void setIndexName(String indexName) {
            this.indexName = indexName;
        }

        public long getCheckTime() {
            return checkTime;
        }

        public WriterKey getKey() {
            return key;
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexName, key);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final TimeIndex other = (TimeIndex) obj;
            if (other.getIndexName() == null || other.getKey() == null || this.indexName == null || this.key == null) {
                return false;
            }
            return (other.getIndexName().equals(this.indexName) && other.getKey().equals(this.key));
        }
    }

    class CheckService extends TimerTask {

        @Override
        public void run() {
            try {
                checkCompacts();
            } catch (Exception e) {
                log.error(e, "error in Check Compacts");
            }
            try {
                checkDeletes();
            } catch (Exception e) {
                log.error(e, "error in Check Deletes");
            }
        }

    }

    class Compaction {
        private List<String> splits;
        private String end;

        Compaction(List<String> splits, String end) {
            this.splits = splits;
            this.end = end;
        }

        public List<String> getSplits() {
            return splits;
        }

        public void setSplits(List<String> splits) {
            this.splits = splits;
        }

        public String getEnd() {
            return end;
        }

        public void setEnd(String end) {
            this.end = end;
        }
    }

}
