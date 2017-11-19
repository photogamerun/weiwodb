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
package com.facebook.presto.lucene;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.weakref.jmx.internal.guava.base.Throwables;

import com.facebook.presto.lucene.base.IndexInfo;
import com.facebook.presto.lucene.base.util.IndexSplitKeyUtil;
import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;
import com.facebook.presto.lucene.util.LuceneUtils;
import com.google.common.base.Joiner;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import io.airlift.log.Logger;

public class RemoteFileSystemClient {

    private static final Logger log = Logger.get(RemoteFileSystemClient.class);

    private Cache<String, List<IndexInfo>> cache = CacheBuilder.newBuilder().maximumSize(1024)
            .expireAfterAccess(15, TimeUnit.SECONDS).build();

    private Configuration hdfsConfig;
    // private ExecutorService hdfsSplitFetcherService;

    public RemoteFileSystemClient(Configuration hdfsConfig) {
        this.hdfsConfig = hdfsConfig;
        // this.hdfsSplitFetcherService = Executors.newFixedThreadPool(8);
    }

    public List<IndexInfo> getHdfsSplit(String dataPath, String schema, String table, String[] partitions)
            throws IOException {
        List<IndexInfo> indexs = new ArrayList<>();
        if (partitions != null) {
            for (String p : partitions) {
                String key = cacheKeyGenarate(schema, table, p);
                List<IndexInfo> list = cache.getIfPresent(key);
                if (list != null) {
                    indexs.addAll(list);
                } else {
                    list = getHdfsSplitForPartition(dataPath, schema, table, p);
                    indexs.addAll(list);
                    if (list.size() > 0) {
                        cache.put(key, list);
                    }
                }
            }
        }
        return indexs;
    }

    private List<IndexInfo> getHdfsSplitForPartition(String dataPath, String schema, String table, String p)
            throws IOException, FileNotFoundException {
        Path tablePath = new Path(new Path(dataPath), schema + LuceneUtils.FILE_SEP + table);
        FileSystem fs = FileSystem.get(hdfsConfig);
        Path partitionPath = new Path(tablePath, p);
        List<IndexInfo> indexs = new ArrayList<>();
        if (fs.exists(partitionPath)) {
            FileStatus[] files = fs.listStatus(partitionPath);
            if (files != null) {
                for (FileStatus file : files) {
                    Path idPath = new Path(partitionPath, file.getPath().getName());
                    Path splitsPath = new Path(idPath, WeiwoDBConfigureKeys.WEIWO_SPLITS_FILE);
                    if (fs.exists(splitsPath)) {
                        List<String> splits = new ArrayList<>();
                        FSDataInputStream in = fs.open(splitsPath, 2048);
                        String split = null;
                        try {
                            while ((split = in.readLine()) != null) {
                                splits.add(split);
                            }
                            for (String str : splits) {
                                indexs.add(new IndexInfo(schema, table, p, file.getPath().getName(), str));
                            }
                        } finally {
                            in.close();
                        }
                    }
                }
            }
        }
        return indexs;
    }

    static String cacheKeyGenarate(String schema, String table, String partition) {
        return Joiner.on(IndexSplitKeyUtil.KEY_SEP).join(schema, table, partition);
    }

    class FetcherTask implements Runnable {

        CountDownLatch latch;
        private List<IndexInfo> splits;
        private Path idPath;
        String schema;
        String table;
        String partition;
        boolean success = false;

        public FetcherTask(Path idPath, CountDownLatch latch, String schema, String table, String partition) {
            this.splits = new ArrayList<>();
            this.idPath = idPath;
            this.latch = latch;
            this.schema = schema;
            this.table = table;
            this.partition = partition;
        }

        @Override
        public void run() {
            try {
                Path splitsPath = new Path(idPath, WeiwoDBConfigureKeys.WEIWO_SPLITS_FILE);
                FileSystem fs;
                try {
                    fs = FileSystem.get(hdfsConfig);
                } catch (IOException e) {
                    log.error(e, "Get FileSystem error");
                    throw Throwables.propagate(e);
                }
                try {
                    if (fs.exists(splitsPath)) {
                        try (FSDataInputStream in = fs.open(splitsPath, 2048)) {
                            String split = null;
                            while ((split = in.readLine()) != null) {
                                splits.add(new IndexInfo(schema, table, partition, idPath.getName(), split));
                            }
                            success = true;
                        } catch (IOException e) {
                            log.error(e, "Read splits file error");
                            throw Throwables.propagate(e);
                        }
                    } else {
                        success = true;
                    }
                } catch (IOException e) {
                    log.error(e, "error");
                    throw Throwables.propagate(e);
                }
            } finally {
                latch.countDown();
            }
        }

        public boolean isSuccess() {
            return success;
        }

        public List<IndexInfo> getSplits() {
            return splits;
        }

    }

}
