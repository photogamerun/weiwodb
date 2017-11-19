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
package com.facebook.presto.weiwo.manager.index;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.inject.Inject;

import com.facebook.presto.lucene.base.util.IndexSplitKeyUtil;

import io.airlift.log.Logger;

public class LuceneIndexManager {
    
    private static final Logger log = Logger.get(LuceneIndexManager.class);

    final ConcurrentHashMap<String, LucenePartitionIndexCache> partitionCache;

    final ReentrantReadWriteLock lock;
    final WriteLock writeLock;
    final ReadLock readLock;

    @Inject
    public LuceneIndexManager() {
        this.partitionCache = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.writeLock = lock.writeLock();
        this.readLock = lock.readLock();
    }

    private void writeLock() {
        this.writeLock.lock();
    }

    private void writeUnLock() {
        this.writeLock.unlock();
    }

    private void readLock() {
        this.readLock.lock();
    }

    private void readUnLock() {
        this.readLock.unlock();
    }

    public void open(IndexInfoWithNodeInfo indexInfoWithNode) {
        requireNonNull(indexInfoWithNode, "indexInfoWithNode is null");
        requireNonNull(indexInfoWithNode.getIndexInfo(), "indexInfo is null");
        requireNonNull(indexInfoWithNode.getNodes(), "nodes is null");
        if(indexInfoWithNode.getNodes().size() != 1){
            log.error("Open index info nodes num not correct." + indexInfoWithNode.getNodes().size());
            return;
        }
        String partitionKey = IndexSplitKeyUtil.getPartitionKey(indexInfoWithNode.getIndexInfo().getSchema(),
                indexInfoWithNode.getIndexInfo().getTable(), indexInfoWithNode.getIndexInfo().getPartition());
        String indexKey = IndexSplitKeyUtil.getIndexSplitKey(indexInfoWithNode.getIndexInfo().getId(),
                indexInfoWithNode.getIndexInfo().getIndexName());
        writeLock();
        try {
            LucenePartitionIndexCache cache = partitionCache.get(partitionKey);
            if (cache == null) {
                cache = new LucenePartitionIndexCache(partitionKey);
                cache.open(indexKey, indexInfoWithNode);
                partitionCache.put(partitionKey, cache);
            } else {
                cache.open(indexKey, indexInfoWithNode);
            }
        } finally {
            writeUnLock();
        }
    }

    public void close(IndexInfoWithNodeInfo indexInfoWithNode) {
        requireNonNull(indexInfoWithNode, "indexInfoWithNode is null");
        requireNonNull(indexInfoWithNode.getIndexInfo(), "indexInfo is null");
        requireNonNull(indexInfoWithNode.getNodes(), "nodes is null");
        if(indexInfoWithNode.getNodes().size() != 1){
            log.error("Open index info nodes num not correct." + indexInfoWithNode.getNodes().size());
            return;
        }
        String partitionKey = IndexSplitKeyUtil.getPartitionKey(indexInfoWithNode.getIndexInfo().getSchema(),
                indexInfoWithNode.getIndexInfo().getTable(), indexInfoWithNode.getIndexInfo().getPartition());
        String indexKey = IndexSplitKeyUtil.getIndexSplitKey(indexInfoWithNode.getIndexInfo().getId(),
                indexInfoWithNode.getIndexInfo().getIndexName());
        writeLock();
        try {
            LucenePartitionIndexCache cache = partitionCache.get(partitionKey);
            if (cache != null) {
                cache.close(indexKey, indexInfoWithNode);
                if(cache.getCacheSize() < 1){
                    partitionCache.remove(partitionKey);
                }
            }
        } finally {
            writeUnLock();
        }
    }

    public List<IndexInfoWithNodeInfo> getIndexList(String db, String table, List<String> partitions) {
        requireNonNull(db, "db is null");
        requireNonNull(table, "table is null");
        requireNonNull(partitions, "partitions is null");
        String key;
        List<IndexInfoWithNodeInfo> result = new ArrayList<>();
        readLock();
        try {
            for (String p : partitions) {
                if (p != null && p != "") {
                    key = IndexSplitKeyUtil.getPartitionKey(db, table, p);
                    LucenePartitionIndexCache pc = partitionCache.get(key);
                    if (pc != null) {
                        result.addAll(pc.getIndexList());
                    }
                }
            }
            return result;
        } finally {
            readUnLock();
        }
    }

}
