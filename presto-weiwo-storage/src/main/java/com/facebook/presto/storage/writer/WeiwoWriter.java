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
package com.facebook.presto.storage.writer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weakref.jmx.internal.guava.base.Joiner;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.storage.WeiwoDBConfigureKeys;
import com.facebook.presto.storage.WeiwoStorageHandler;
import com.facebook.presto.storage.hdfs.HdfsDirectory;

import static java.util.Objects.requireNonNull;

public class WeiwoWriter implements RecordWriter{
	private static final Logger logger = LoggerFactory.getLogger(WeiwoWriter.class.getName());
	public static final String KEY_SEP = "_-_";
	public static final String KEY_VALUE_SEPERATOR = "mapreduce.output.textoutputformat.separator";
	
    private static String db;
    private static String table;
//    private final String partition;
    private final String id;
	private final String appid;
	private final String attemptid;
    private final String[] fields;
    private final String[] fieldTypes;
    private final long realIndexMaxSize;
    private final long realIndexMaxTime;
    
    private WeiwoDBObjectCache<String, Field> fieldCache;
    private Map<String, IndexWriter> cache = new HashMap<>();
    private Map<String, WeiwoDBSplitsLinkOperator> operatorCache = new HashMap<>();
    private Map<String, String> indexCache = new HashMap<>();
    
    private FileSystem fs;
    private WeiwoDBSplitsLinkOperator operator;
    
    private JobConf jobConf;

	private String location;
    
    public WeiwoWriter(JobConf jobConf, Properties parameters) {
		this.jobConf = jobConf;
		// init indexwriter other args
		realIndexMaxSize = Long.parseLong(parameters.getProperty("lucene.realtime.index.max.size.mb", "256")) * 1024 * 1024;
		realIndexMaxTime = Long.parseLong(parameters.getProperty("lucene.realtime.index.max.time.sec", "900")) * 1000;
		db = "default";
		String tableName =
  		      parameters.getProperty(hive_metastoreConstants.META_TABLE_NAME).toLowerCase();
		if (tableName != null && tableName.indexOf(".") > 0) {
			db = tableName.substring(0,tableName.indexOf("."));
			table = tableName.substring(tableName.indexOf(".") + 1);
		}
		String mapping = parameters.getProperty(WeiwoStorageHandler.WEIWODB_SCHEMA_MAPPING_KEY);
        String[] strs = mapping.split("\\s*,\\s*");
    	Map<String, String> columns = new HashMap<>(strs.length);
    	fields = new String[strs.length];
        fieldTypes = new String[strs.length];
    	int i = 0;
    	for(String str:strs)
    	{
    		String[] kvs = str.split(" ");
    		columns.put(kvs[0], kvs[1]);
    		fields[i] = kvs[0];
    		fieldTypes[i] = kvs[1];
            i++;
    	}
    	
    	id = jobConf.get("mapreduce.task.id");
		appid = jobConf.get("mapreduce.job.id");
		attemptid = jobConf.get("mapreduce.task.attempt.id");
		String separator = jobConf.get(KEY_VALUE_SEPERATOR);
    	fieldCache = new WeiwoDBObjectCache<>();
		location = parameters.getProperty(WeiwoStorageHandler.WEIWODB_DATA_LOCATION,"/data/weiwodb");
		logger.info("appid-" + appid + ";taskid-" + id + ";attemptid-" + attemptid + ";separator-" + separator);
    	try {
			if(location.indexOf("://") > 0 || location.indexOf(":") > 0) {
				fs = FileSystem.get(new URI(location), jobConf);
			}else
			{
				fs = FileSystem.get(jobConf);
			}
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		} catch (URISyntaxException e) {
			logger.error(e.getMessage(),e);
		}
	}

	public void write(JSONObject object) throws IOException {
		String partition = object.getString(LuceneColumnUtil.PARTITION_KEY);
		IndexWriter writer;
		try {
			writer = getIndexWriter(partition);
			int columnSize = fields.length;
			Document doc = new Document();
			for (int i = 0; i < columnSize; i++) {
				if (!LuceneColumnUtil.isPartitionKey(fields[i])) {
					List<Field> fl = createIndexField(fields[i], fieldTypes[i], object);
					for (Field f : fl) {
						doc.add(f);
					}
				}
			}
			writer.addDocument(doc);
		} catch (Exception e) {
			throw new IOException("IndexWriter write error:",e);
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
	
	private IndexWriter getIndexWriter(String partition) throws Exception {
		String key = genarateKey(db, table, partition, attemptid);
		if(cache.containsKey(key)) return cache.get(key);
		else
		{
	    	Path hdfsDirPath = new Path(new Path(location),
	                Joiner.on(WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR).join(db, table, partition, appid, attemptid));
	    	HdfsDirectory directory = null;
			try {
				directory = new HdfsDirectory(key,hdfsDirPath, jobConf, fs);
				IndexWriterConfig luceneConfig = createConfig();
				IndexWriter writer = new IndexWriter(directory, luceneConfig);
				cache.put(key, writer);
				indexCache.put(key, attemptid);
				
				//init operator
				String key1 = key.substring(0,key.length()-11);
				if(!operatorCache.containsKey(key1))
				{
					String file = location + WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR + db
							+ WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR + table + WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR
							+ partition + WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR + appid
							+ WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR + WeiwoDBConfigureKeys.WEIWO_SPLITS_FILE;
					operator = new WeiwoDBSplitsLinkOperator(fs, file, db,table,partition,appid);
					operatorCache.put(key1, operator);
				}
				return writer;
			}  catch (IOException e) {
				logger.error("HiveDataWriter init error.",e);
	            try {
	                if (directory != null) {
	                    directory.close();
	                }
	            } catch (IOException e2) {
	            	logger.error("HiveDataWriter error init close directory.",e2);
	            }
	            try {
	                if (fs.exists(hdfsDirPath)) {
	                    fs.delete(hdfsDirPath, true);
	                }
	            } catch (IOException e2) {
	            	logger.error("HiveDataWriter error init delete hdfsDirPath=" + hdfsDirPath,e2);
	            }
	            throw e;
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
	
	private boolean checkNeedFlushAndClose(IndexWriter writer,Condition condition) {
        if (condition.getSize().get() > realIndexMaxSize || (System.currentTimeMillis() - condition.getTime()) > realIndexMaxTime) {
            return true;
        } else {
            return false;
        }
    }
	
	public static String genarateKey(String db, String table, String partition,
			String id) {
		requireNonNull(db, "db is null");
		requireNonNull(table, "table is null");
		requireNonNull(partition, "partition is null");
		requireNonNull(id, "id is null");
		return Joiner.on(KEY_SEP).join(db, table, partition, id);
	}
	
	private void flushAndClose(IndexWriter writer, String key) {
		logger.info("Flush and close writer. " + key);
		try {
			writer.flush();
			writer.commit();
			writer.close();
			String key1 = key.substring(0,key.length()-11);
			operatorCache.get(key1).addSplit(indexCache.get(key));
			cache.remove(key);
//			ccache.remove(key);
			indexCache.remove(key);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	@Override
	public void write(Writable w) throws IOException {
		if(w instanceof JSONWritable)
		{
			JSONWritable jsonWritable = (JSONWritable) w;
			write(jsonWritable.getJson());
		}
		if(w instanceof Text)
		{
			Text text = (Text) w;
			String[] ts = text.toString().split("\001");
			if(ts.length != fields.length)
			{
				throw new IOException("Result mismatch fields!");
			}
			else
			{
				Map<String, String> map = new HashMap<>(ts.length);
				for (int i = 0; i < ts.length; i++) {
					map.put(fields[i], ts[i]);
				}
				String json = JSON.toJSONString(map);
				write(JSON.parseObject(json));
			}
		}
	}

	@Override
	public void close(boolean abort) throws IOException {
		logger.info("task abort:" + abort + ",size:" + cache.size() + "--");
		try {
			for(Map.Entry<String, IndexWriter> e: cache.entrySet())
			{
				IndexWriter writer = e.getValue();
//				writer.flush();
//				writer.commit();
				writer.close();
				String key1 = e.getKey().substring(0,e.getKey().length()-11);
				operatorCache.get(key1).addSplit(indexCache.get(e.getKey()));
			}
			cache.clear();
			operatorCache.clear();
			indexCache.clear();
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
			throw new IOException("indexwriter close fail",e);
		}
	}
	
	private class Condition{
		private AtomicLong size = new AtomicLong(0);
	    private long time = 0L;
	    
		public Condition(long time) {
			this.time = time;
		}
		public AtomicLong getSize() {
			return size;
		}
		public long getTime() {
			return time;
		}
	}
}
