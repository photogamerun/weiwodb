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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.facebook.presto.storage.WeiwoDBConfigureKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Splits Link Operator 操作splits
 * link文件,每个{datapath}/{db}/{table}/{partition}/{nodeid}下面会有相应的indexLink文件
 * 记录合法的当前已经commit到hdfs的索引split,hdfs目录下的索引分片数在合并index之后的一段时间会比indexLink记录的多
 * (有些split已经合并到新的split了,老的split会延时删除)
 * 
 * @author folin01.chen
 *
 */
public class WeiwoDBSplitsLinkOperator {

    private static final Logger logger = LoggerFactory.getLogger(WeiwoDBSplitsLinkOperator.class);
    private final FileSystem fs;
    private final String file;
    WriterKey key;
    private final Path lock;

    public WeiwoDBSplitsLinkOperator(FileSystem fs, String file, String schema, String table, String partition,
            String id) {
        this(fs,file,new WriterKey(schema, table, partition, id));
    }

    public WeiwoDBSplitsLinkOperator(FileSystem fs, String file, WriterKey key) {
        this.fs = requireNonNull(fs, "fs is null");
        this.file = requireNonNull(file, "file is null");
        this.key = requireNonNull(key, "key is null");
        this.lock = new Path(file + ".lock");
    }

	public synchronized void addSplit(String indexName) throws IOException {
        logger.info("Add split:" + indexName);
        while (!SplitLinkOperatorLock.lock(fs,lock)) {
            try {
                wait(50);
            } catch (InterruptedException e) {
            }
        }

        try {
            Map<String,String> splits = new HashMap<>();
            if (fs.exists(new Path(file))) {
                FSDataInputStream in = fs.open(new Path(file), 2048);
                String split = null;
                try {
                    while ((split = in.readLine()) != null) {
                        splits.put(split.substring(0,split.length()-2),split);
                    }
                } finally {
                    in.close();
                }
            }
            if(!splits.containsKey(indexName.substring(0,indexName.length()-2)))
            {
                splits.put(indexName.substring(0,indexName.length()-2),indexName);
                FSDataOutputStream out = fs.create(new Path(file + WeiwoDBConfigureKeys.WEIWO_SPLITS_FILE_TMP), true);
                try {
                    for (Map.Entry<String,String> str : splits.entrySet()) {
                        out.write((str.getValue() + "\n").getBytes(Charset.forName("utf-8")));
                    }
                } finally {
                    out.flush();
                    out.close();
                }
                fs.delete(new Path(file),false);
                fs.rename(new Path(file + WeiwoDBConfigureKeys.WEIWO_SPLITS_FILE_TMP), new Path(file));
            }

        } finally {
            SplitLinkOperatorLock.unLock(fs,lock);
        }

    }
    
	public synchronized void deleteAndAddSplit(List<String> deletes, String add) throws IOException {

        while (!SplitLinkOperatorLock.lock(key)) {
            try {
                wait(50);
            } catch (InterruptedException e) {
            }
        }

        try {
            Set<String> splits = new HashSet<>();
            splits.add(add);
            if (fs.exists(new Path(file))) {
                FSDataInputStream in = fs.open(new Path(file), 2048);
                String split = null;
                try {
                    while ((split = in.readLine()) != null) {
                        splits.add(split);
                    }
                } finally {
                    in.close();
                }
            }
            splits.removeAll(deletes);
            FSDataOutputStream out = fs.create(new Path(file + WeiwoDBConfigureKeys.WEIWO_SPLITS_FILE_TMP), true);
            try {
                for (String str : splits) {
                    out.write((str + "\n").getBytes(Charset.forName("utf-8")));
                }
            } finally {
                out.close();
            }
            fs.delete(new Path(file),false);
            fs.rename(new Path(file + WeiwoDBConfigureKeys.WEIWO_SPLITS_FILE_TMP), new Path(file));
        } finally {
            SplitLinkOperatorLock.unLock(key);
        }

    }

    public synchronized void deleteSplit(String indexName) throws IllegalArgumentException, IOException {
        while (!SplitLinkOperatorLock.lock(key)) {
            try {
                wait(50);
            } catch (InterruptedException e) {
            }
        }

        try {
            Set<String> splits = new HashSet<>();
            if (fs.exists(new Path(file))) {
                FSDataInputStream in = fs.open(new Path(file), 2048);
                String split = null;
                try {
                    while ((split = in.readLine()) != null) {
                        splits.add(split);
                    }
                } finally {
                    in.close();
                }
            }
            splits.remove(indexName);
            FSDataOutputStream out = fs.create(new Path(file + WeiwoDBConfigureKeys.WEIWO_SPLITS_FILE_TMP), true);
            try {
                for (String str : splits) {
                    out.write((str + "\n").getBytes(Charset.forName("utf-8")));
                }
            } finally {
                out.close();
            }
            fs.delete(new Path(file),false);
            fs.rename(new Path(file + WeiwoDBConfigureKeys.WEIWO_SPLITS_FILE_TMP), new Path(file));
        } finally {
            SplitLinkOperatorLock.unLock(key);
        }
    }
    
    static class WriterKey {
		String db;
		String table;
		String partition;
		String id;

		public WriterKey(String db, String table, String partition, String id) {
			this.db = db;
			this.table = table;
			this.partition = partition;
			this.id = id;
		}

		public String getDb() {
			return db;
		}

		public void setDb(String db) {
			this.db = db;
		}

		public String getTable() {
			return table;
		}

		public void setTable(String table) {
			this.table = table;
		}

		public String getPartition() {
			return partition;
		}

		public void setPartition(String partition) {
			this.partition = partition;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

//		@Override
//		public int hashCode() {
//			return Objects.hash(db, table, partition, id);
//		}
//		
//		@Override
//		public boolean equals(Object obj) {
//			if (this == obj) {
//				return true;
//			}
//			if (obj == null || getClass() != obj.getClass()) {
//				return false;
//			}
//			final WriterKey other = (WriterKey) obj;
//			if (other.db == null || other.table == null
//					|| other.partition == null || other.id == null
//					|| this.db == null || this.table == null
//					|| this.partition == null || this.id == null) {
//				return false;
//			}
//			return (other.db.equals(this.db) && other.table.equals(this.table)
//					&& other.partition.equals(this.partition)
//					&& other.id.equals(this.id));
//		}
	}

}
