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
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;
import com.facebook.presto.lucene.services.writer.WeiwoDBSplitWriterManager.WriterKey;

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

    private final FileSystem fs;
    private final String file;
    WriterKey key;

    public WeiwoDBSplitsLinkOperator(FileSystem fs, String file, String schema, String table, String partition,
            String id) {
        this.fs = requireNonNull(fs, "fs is null");
        this.file = requireNonNull(file, "file is null");
        this.key = new WriterKey(schema, table, partition, id);
    }

    public WeiwoDBSplitsLinkOperator(FileSystem fs, String file, WriterKey key) {
        this.fs = requireNonNull(fs, "fs is null");
        this.file = requireNonNull(file, "file is null");
        this.key = requireNonNull(key, "key is null");
    }

    public synchronized void addSplit(String indexName) throws IOException {

        while (!SplitLinkOperatorLock.lock(key)) {
            try {
                wait(50);
            } catch (InterruptedException e) {
            }
        }

        try {
            Set<String> splits = new HashSet<>();
            splits.add(indexName);
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
            FSDataOutputStream out = fs.create(new Path(file + WeiwoDBConfigureKeys.WEIWO_SPLITS_FILE_TMP), true);
            try {
                for (String str : splits) {
                    out.write((str + "\n").getBytes(Charset.forName("utf-8")));
                }
            } finally {
                out.close();
            }
            fs.delete(new Path(file));
            fs.rename(new Path(file + WeiwoDBConfigureKeys.WEIWO_SPLITS_FILE_TMP), new Path(file));
        } finally {
            SplitLinkOperatorLock.unLock(key);
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
            fs.delete(new Path(file));
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
            fs.delete(new Path(file));
            fs.rename(new Path(file + WeiwoDBConfigureKeys.WEIWO_SPLITS_FILE_TMP), new Path(file));
        } finally {
            SplitLinkOperatorLock.unLock(key);
        }
    }

}
