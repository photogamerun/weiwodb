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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.lucene.IndexManagerClient;
import com.facebook.presto.lucene.LuceneConfig;
import com.facebook.presto.lucene.WeiwoDBType;
import com.facebook.presto.lucene.ZkClientFactory;
import com.facebook.presto.lucene.base.IndexInfo;
import com.facebook.presto.lucene.base.IndexInfoWithNodeInfoSimple;
import com.facebook.presto.lucene.base.WeiwoDBMemoryInfo;
import com.facebook.presto.lucene.base.WeiwoDBNodeInfo;
import com.facebook.presto.lucene.base.util.IndexSplitKeyUtil;
import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;
import com.facebook.presto.lucene.hdfs.HdfsDirectory;
import com.facebook.presto.lucene.services.writer.exception.WriterCommitException;
import com.facebook.presto.lucene.services.writer.exception.WriterDeleteException;
import com.facebook.presto.lucene.services.writer.exception.WriterRegisterException;
import com.facebook.presto.lucene.services.writer.util.IKAnalyzerUtil;
import com.facebook.presto.lucene.services.writer.util.IndexNameGenarator;
import com.facebook.presto.lucene.util.JsonCodecUtils;
import com.facebook.presto.lucene.util.LuceneColumnUtil;
import com.facebook.presto.spi.Node;
import com.google.common.base.Joiner;

import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

/**
 * Split Writer 每个分片的Writer,使用db(数据库) table(表名) partition(分区) nodeid(节点id)
 * indexName(索引名) 5个参数来标识一个split
 * 
 * @author folin01.chen
 *
 */
public class WeiwoDBSplitNormalWriter implements WeiwoDBSplitWriter {

    private static final Logger log = Logger.get(WeiwoDBSplitNormalWriter.class);

    final LuceneConfig config;
    final String schema;
    final String table;
    final String partition;
    final String id;
    final String nodeId;
    final ZkClientFactory zkClientFactory;
    final String[] fields;
    final String[] fieldTypes;
    final String writingPath;
    final String alivePath;
    final String indexName;
    final Configuration conf;
    final FileSystem fs;
    final Node hostAddress;

    private HdfsDirectory directory;
    private RAMDirectory mdirectory;
    private IndexWriter writer;
    private IndexWriter mwriter;
    private Long lastOpenTime = 0L;
    private AtomicLong dataSize = new AtomicLong(0);
    private AtomicInteger currentWriteThread = new AtomicInteger(0);
    private long createTime = 0L;
    private boolean done = false;
    private boolean splitAdded = false;
    private final long realIndexMaxSize;
    private final long realIndexMaxTime;
    private WeiwoDBObjectCache<String, Field> fieldCache;
    private DirectoryReader reader;
    public final JsonCodec<IndexInfoWithNodeInfoSimple> indexInfoCodec;
    private final WeiwoDBSplitWal walWriter;
    private final WeiwoDBSplitsLinkOperator splitsInfoOperator;
    private AtomicInteger readerRefCount = new AtomicInteger(0);
    private final Path hdfsDirPath;
    private final ScheduledExecutorService closeService;
    private boolean isClosed = false;
    private IndexManagerClient indexManagerClient;

    Map<String, String> luceneFields;

    public WeiwoDBSplitNormalWriter(LuceneConfig config, String schema, String table, String partition, String id,
            ZkClientFactory zkClientFactory, Map<String, String> luceneFields, Configuration conf, FileSystem fs,
            HttpClient httpClient, WeiwoDBSplitWal walWriter, Node hostAddress, ScheduledExecutorService closeService) {
        this.hostAddress = hostAddress;
        this.indexManagerClient = new IndexManagerClient(config, httpClient);
        this.indexInfoCodec = JsonCodecUtils.getIndexInfoCodec();
        this.config = requireNonNull(config, "config is null");
        this.realIndexMaxTime = config.getRealIndexMaxTime() * 1000;
        this.realIndexMaxSize = config.getRealIndexMaxSize() * 1024 * 1024;
        this.walWriter = requireNonNull(walWriter, "walWriter is null");
        this.splitsInfoOperator = new WeiwoDBSplitsLinkOperator(fs,
                config.getDataPath() + WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR + schema
                        + WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR + table + WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR
                        + partition + WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR + id
                        + WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR + WeiwoDBConfigureKeys.WEIWO_SPLITS_FILE,
                schema, table, partition, id);
        this.schema = requireNonNull(schema, "db is null");
        this.table = requireNonNull(table, "table is null");
        this.partition = requireNonNull(partition, "partiton is null");
        this.id = requireNonNull(id, "id is null");
        this.nodeId = WriterIdUtil.split(id)[0];
        this.zkClientFactory = requireNonNull(zkClientFactory, "zkClientFactory is null");
        this.conf = requireNonNull(conf, "conf is null");
        this.fs = requireNonNull(fs, "fs is null");
        this.closeService = requireNonNull(closeService, "closeService is null");
        this.luceneFields = requireNonNull(luceneFields, "luceneFields is null");
        this.fields = new String[luceneFields.size()];
        this.fieldTypes = new String[luceneFields.size()];
        Set<String> keys = luceneFields.keySet();
        int i = 0;
        for (String key : keys) {
            fields[i] = key;
            fieldTypes[i] = luceneFields.get(key);
            i++;
        }
        this.writingPath = Joiner.on(WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR).join(config.getZkPath(),
                WeiwoDBConfigureKeys.WEIWO_WRITER_PATH, nodeId, WeiwoDBConfigureKeys.WEIWO_WRITER_WRITING_PATH);
        this.alivePath = Joiner.on(WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR).join(config.getZkPath(),
                WeiwoDBConfigureKeys.WEIWO_WRITER_PATH, nodeId, WeiwoDBConfigureKeys.WEIWO_WRITER_ALIVE_PATH);

        this.indexName = IndexNameGenarator.genarateIndexName();

        this.hdfsDirPath = new Path(new Path(config.getDataPath()),
                Joiner.on(WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR).join(schema, table, partition, id, indexName));
        this.fieldCache = new WeiwoDBObjectCache<>();
    }

    @Override
    public void init() throws IOException {
        ZkClient zkClient = zkClientFactory.getZkClient();
        if (!zkClient.exists(writingPath)) {
            zkClient.createPersistent(writingPath, true);
        }
        if (!zkClient.exists(alivePath)) {
            zkClient.createPersistent(alivePath, true);
        }
        JSONObject json = new JSONObject(getWriterProperties());
        zkClient.createPersistent(writingPath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + indexName, json.toString());
        zkClient.createEphemeral(alivePath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + indexName);

        try {
            String key = IndexSplitKeyUtil.getIndexInfo(schema, table, partition, id, indexName);
            this.directory = new HdfsDirectory(key, hdfsDirPath, conf, fs);
            this.mdirectory = new RAMDirectory();
            IndexWriterConfig luceneConfig = createConfig();
            IndexWriterConfig mluceneConfig = createConfig();
            this.writer = new IndexWriter(directory, luceneConfig);
            this.mwriter = new IndexWriter(mdirectory, mluceneConfig);
            this.reader = DirectoryReader.open(this.mwriter);
            this.createTime = System.currentTimeMillis();
        } catch (IOException e) {
            log.error(e, "WeiwoDBIndexWriter init error.");
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e2) {
                log.error(e2, "WeiwoDBIndexWriter error init close reader.");
            }
            try {
                if (writer != null) {
                    writer.close();
                    mwriter.close();
                }
            } catch (IOException e2) {
                log.error(e2, "WeiwoDBIndexWriter error init close writer.");
            }
            try {
                if (directory != null) {
                    directory.close();
                    mdirectory.close();
                }
            } catch (IOException e2) {
                log.error(e2, "WeiwoDBIndexWriter error init close directory.");
            }
            try {
                if (fs.exists(hdfsDirPath)) {
                    fs.delete(hdfsDirPath, true);
                }
            } catch (IOException e2) {
                log.error(e2, "WeiwoDBIndexWriter error init delete hdfsDirPath=" + hdfsDirPath);
            }
            try {
                zkClient.delete(writingPath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + indexName);
                zkClient.delete(alivePath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + indexName);
            } catch (Exception e2) {
                log.error(e2,
                        "WeiwoDBIndexWriter error init delete writingPath=" + writingPath
                                + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + indexName + ",alivePath=" + alivePath
                                + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + indexName);
            }
            throw e;
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
        luceneConfig.setRAMBufferSizeMB(config.getRealIndexMaxSize() * 2);
        luceneConfig.setMaxBufferedDocs(WeiwoDBConfigureKeys.MAX_BUFFER_DOCS);
        return luceneConfig;
    }

    private Map<String, Object> getWriterProperties() {
        Map<String, Object> map = new HashMap<>();
        map.put("db", schema);
        map.put("table", table);
        map.put("partition", partition);
        map.put("id", id);
        map.put("indexName", indexName);
        map.put("dataPath", config.getDataPath());
        map.put("luceneFields", luceneFields);
        return map;
    }

    public boolean checkNeedFlushAndClose() {
        if (dataSize.get() > realIndexMaxSize || (System.currentTimeMillis() - createTime) > realIndexMaxTime
                || mwriter.maxDoc() >= WeiwoDBConfigureKeys.MAX_BUFFER_DOCS) {
            return true;
        } else {
            return false;
        }
    }

    public void writeWal(String data) throws IOException {
        walWriter.write(data);
    }

    public void write(JSONArray datas, long dataBytes) throws IOException {
        int columnSize = fields.length;
        if (datas != null) {
            int num = datas.size();
            if (num > 0) {
                for (int j = 0; j < num; j++) {
                    JSONObject data = datas.getJSONObject(j);
                    Document doc = new Document();
                    for (int i = 0; i < columnSize; i++) {
                        if (!LuceneColumnUtil.isPartitionKey(fields[i])) {
                            List<Field> fl = createIndexField(fields[i], fieldTypes[i], data);
                            for (Field f : fl) {
                                doc.add(f);
                            }
                        }
                    }
                    writer.addDocument(doc);
                    mwriter.addDocument(doc);
                }
                dataSize.addAndGet(dataBytes);
            }
        }
    }

    public List<Field> createIndexField(String name, String type, JSONObject json) {
        List<Field> list = new ArrayList<>();
        if (WeiwoDBType.getType(type).equals(WeiwoDBType.INT)) {
            Integer value = json.getInteger(name);
            if (value != null) {
                Field ip = fieldCache.getObject(name);
                if (ip != null) {
                    ip.setIntValue(value);// TODO
                } else {
                    ip = new IntPoint(name, value);
                }
                list.add(ip);
                fieldCache.cacheObject(name, ip);

                Field sort = fieldCache.getObject(name + WeiwoDBConfigureKeys.NUMERIC_KEY);

                if (sort != null) {
                    sort.setLongValue(value);
                } else {
                    sort = new NumericDocValuesField(name, value);
                }

                list.add(sort);

                fieldCache.cacheObject(name + WeiwoDBConfigureKeys.NUMERIC_KEY, sort);
            }
        } else if (WeiwoDBType.getType(type).equals(WeiwoDBType.LONG)) {
            Long value = json.getLong(name);
            if (value != null) {
                Field lp = fieldCache.getObject(name);
                if (lp != null) {
                    lp.setLongValue(value);
                } else {
                    lp = new LongPoint(name, value);
                }
                list.add(lp);

                fieldCache.cacheObject(name, lp);

                Field sort = fieldCache.getObject(name + WeiwoDBConfigureKeys.NUMERIC_KEY);

                if (sort != null) {
                    sort.setLongValue(value);
                } else {
                    sort = new NumericDocValuesField(name, value);
                }

                list.add(sort);

                fieldCache.cacheObject(name + WeiwoDBConfigureKeys.NUMERIC_KEY, sort);

            }
        } else if (WeiwoDBType.getType(type).equals(WeiwoDBType.FLOAT)) {
            Float value = json.getFloat(name);
            if (value != null) {
                Field lp = fieldCache.getObject(name);
                if (lp != null) {
                    lp.setFloatValue(value);
                } else {
                    lp = new FloatPoint(name, value);
                }
                list.add(lp);

                fieldCache.cacheObject(name, lp);

                Field sort = fieldCache.getObject(name + WeiwoDBConfigureKeys.SORTED_KEY);

                int floatbits = Float.floatToIntBits(value);
                if (sort != null) {
                    sort.setLongValue(floatbits);
                } else {
                    sort = new NumericDocValuesField(name, floatbits);
                }

                list.add(sort);

                fieldCache.cacheObject(name + WeiwoDBConfigureKeys.SORTED_KEY, sort);

            }
        } else if (WeiwoDBType.getType(type).equals(WeiwoDBType.DOUBLE)) {
            Double value = json.getDouble(name);
            if (value != null) {
                Field dp = fieldCache.getObject(name);
                if (dp != null) {
                    dp.setDoubleValue(value);
                } else {
                    dp = new DoublePoint(name, value);
                }
                list.add(dp);
                fieldCache.cacheObject(name, dp);

                Field sort = fieldCache.getObject(name + WeiwoDBConfigureKeys.NUMERIC_KEY);

                long longbits = Double.doubleToLongBits(value);
                if (sort != null) {
                    sort.setLongValue(longbits);
                } else {
                    sort = new NumericDocValuesField(name, longbits);
                }

                list.add(sort);

                fieldCache.cacheObject(name + WeiwoDBConfigureKeys.NUMERIC_KEY, sort);
            }
        } else if (WeiwoDBType.getType(type).equals(WeiwoDBType.STRING)) {
            String value = json.getString(name);
            if (value != null) {
                Field str = fieldCache.getObject(name);
                if (str != null) {
                    str.setStringValue(value);
                } else {
                    str = new StringField(name, value, Store.NO);
                }
                list.add(str);
                fieldCache.cacheObject(name, str);
                Field strs = fieldCache.getObject(name + WeiwoDBConfigureKeys.SORTED_KEY);
                if (strs != null) {
                    strs.setBytesValue(new BytesRef(value));
                } else {
                    strs = new SortedDocValuesField(name, new BytesRef(value));
                }
                list.add(strs);
                fieldCache.cacheObject(name + WeiwoDBConfigureKeys.SORTED_KEY, strs);
            }
        } else if (WeiwoDBType.getType(type).equals(WeiwoDBType.TIMESTAMP)) {
            Long value = json.getLong(name);
            if (value != null) {
                Field lp = fieldCache.getObject(name);
                if (lp != null) {
                    lp.setLongValue(value);
                } else {
                    lp = new LongPoint(name, value);
                }
                list.add(lp);

                fieldCache.cacheObject(name, lp);

                Field sort = fieldCache.getObject(name + WeiwoDBConfigureKeys.NUMERIC_KEY);

                if (sort != null) {
                    sort.setLongValue(value);
                } else {
                    sort = new NumericDocValuesField(name, value);
                }

                list.add(sort);

                fieldCache.cacheObject(name + WeiwoDBConfigureKeys.NUMERIC_KEY, sort);

            }
        } else if (WeiwoDBType.getType(type).equals(WeiwoDBType.TEXT)) {
            String value = json.getString(name);
            if (value != null) {
                Field str = fieldCache.getObject(name);
                if (str != null) {
                    str.setStringValue(value);
                } else {
                    str = new TextField(name, value, Store.YES);
                }
                list.add(str);
                fieldCache.cacheObject(name, str);
            }
        }
        return list;
    }

    @Override
    public IndexReader getReader() throws IOException {
        synchronized (lastOpenTime) {
            Long now = System.currentTimeMillis();
            if ((now - lastOpenTime) > (config.getReaderOpenDelay() * 1000)) {
                DirectoryReader newReader = null;
                if (writer.isOpen()) {
                    newReader = DirectoryReader.openIfChanged(this.reader, this.mwriter);
                }
                if (newReader != null) {
                    this.reader = newReader;
                    this.lastOpenTime = now;
                }
            }
        }
        readerRefCount.incrementAndGet();
        return this.reader;
    }

    @Override
    public void returnReader(IndexReader reader) throws IOException {
        readerRefCount.decrementAndGet();
    }

    @Override
    public int getReaderRefCount() {
        return readerRefCount.get();
    }

    public synchronized void addSplitInfo() {
        if (splitAdded) {
            return;
        }
        try {
            splitsInfoOperator.addSplit(indexName);
            splitAdded = true;
        } catch (IOException e) {
            log.error(e, "Add split info to file weiwo.splits error");
        }
    }

    @Override
    public synchronized void commit() throws WriterCommitException {
        if (done) {
            return;
        }
        try {
            writer.flush();
            writer.commit();
            writer.close();
        } catch (IOException e) {
            throw new WriterCommitException(e);
        } finally {
            done = true;
        }
    }

    @Override
    public void deleteZK() throws WriterDeleteException {
        try {
            zkClientFactory.getZkClient().delete(writingPath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + indexName);
            zkClientFactory.getZkClient().delete(alivePath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + indexName);
        } catch (Exception e) {
            throw new WriterDeleteException(e);
        }
    }

    @Override
    public void closeMem() throws IOException {
        try {
            mwriter.close();
        } catch (Exception e) {
            //
        }
        reader.close();
        mdirectory.close();
    }

    @Override
    public void close() throws IOException {
        try {
            writer.close();
        } catch (Exception e) {
            //
        }
        try {
            directory.close();
        } finally {
            isClosed = true;
        }
    }

    @Override
    public void deleteHdfs() throws WriterDeleteException {
        try {
            if (fs.exists(hdfsDirPath)) {
                fs.delete(hdfsDirPath, true);
            }
        } catch (IOException e) {
            throw new WriterDeleteException(e);
        }
    }

    public void registerWriter() throws IOException {
        IndexInfo indexInfo = new IndexInfo(schema, table, partition, id, indexName);
        indexManagerClient.registerWriter(createIndexInfo(indexInfo));
    }

    public void unRegisterWriter() throws WriterRegisterException {
        IndexInfo indexInfo = new IndexInfo(schema, table, partition, id, indexName);
        indexManagerClient.unRegisterWriter(createIndexInfo(indexInfo));

    }

    private IndexInfoWithNodeInfoSimple createIndexInfo(IndexInfo index) {
        WeiwoDBNodeInfo node = new WeiwoDBNodeInfo(config.getNodeId(), new WeiwoDBMemoryInfo(0, 0, 0),
                hostAddress.getHostAndPort(), hostAddress.getHttpUri().toString());
        List<WeiwoDBNodeInfo> nodes = new ArrayList<>();
        nodes.add(node);
        requireNonNull(index, "index is null");
        return new IndexInfoWithNodeInfoSimple(index, nodes);
    }

    @Override
    public WeiwoDBSplitWal getWal() {
        return walWriter;
    }

    public ScheduledExecutorService getCloseService() {
        return closeService;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public long getTotalSize() {
        return mdirectory.ramBytesUsed();
    }
}
