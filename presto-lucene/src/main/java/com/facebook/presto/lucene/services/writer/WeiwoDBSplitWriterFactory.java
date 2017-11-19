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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.facebook.presto.lucene.LuceneConfig;
import com.facebook.presto.lucene.ZkClientFactory;
import com.facebook.presto.spi.Node;

import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;

/**
 * Split Writer工厂
 * 
 * @author folin01.chen
 *
 */
public class WeiwoDBSplitWriterFactory {

    private static final Logger log = Logger.get(WeiwoDBSplitWriterFactory.class);

    public WeiwoDBSplitRecoverWriter createRecover(LuceneConfig config, String db, String table, String partition,
            String id, ZkClientFactory zkClientFactory, Map<String, String> luceneFields, String indexName,
            Configuration conf, FileSystem fs, HttpClient httpClient, WeiwoDBSplitWal walReader, Node node,
            CallBack callBack) {
        WeiwoDBSplitRecoverWriter writer = new WeiwoDBSplitRecoverWriter(config, db, table, partition, id,
                zkClientFactory, luceneFields, indexName, conf, fs, httpClient, walReader, node, callBack);
        try {
            writer.init();
        } catch (IOException e) {
            log.error(e, "Init split writer failed . SplitWriter = "
                    + WeiwoDBSplitWriterManager.genarateKey(db, table, partition, id));
            return null;
        }
        try {
            writer.registerWriter();
        } catch (IOException e) {
            log.error(e, "Register split writer failed . SplitWriter = "
                    + WeiwoDBSplitWriterManager.genarateKey(db, table, partition, id));
            writer.recoverThread.interrupt();
            try {
                writer.commit();
            } catch (IOException e1) {
                log.error(e1, "Commit error.");
            }
            try {
                writer.close();
            } catch (IOException e1) {
                log.error(e1, "close error.");
            }
            try {
                writer.closeMem();
            } catch (IOException e1) {
                log.error(e1, "closeMem error.");
            }
            try {
                writer.deleteZK();
            } catch (IOException e1) {
                log.error(e1, "deleteZK error.");
            }
            try {
                writer.deleteHdfs();
            } catch (IOException e1) {
                log.error(e1, "deleteHdfs error.");
            }
            return null;
        }
        return writer;
    }

    public WeiwoDBSplitNormalWriter createNormal(LuceneConfig config, String db, String table, String partition,
            String id, ZkClientFactory zkClientFactory, Map<String, String> luceneFields, Configuration conf,
            FileSystem fs, HttpClient httpClient, WeiwoDBSplitWal walWriter, Node node,
            ScheduledExecutorService closeService) {
        WeiwoDBSplitNormalWriter writer = new WeiwoDBSplitNormalWriter(config, db, table, partition, id,
                zkClientFactory, luceneFields, conf, fs, httpClient, walWriter, node, closeService);
        try {
            writer.init();
        } catch (IOException e) {
            log.error(e, "Init split writer failed . SplitWriter = "
                    + WeiwoDBSplitWriterManager.genarateKey(db, table, partition, id));
            return null;
        }
        try {
            writer.registerWriter();
        } catch (IOException e) {
            log.error(e, "Register split writer failed . SplitWriter = "
                    + WeiwoDBSplitWriterManager.genarateKey(db, table, partition, id));
            try {
                writer.commit();
            } catch (IOException e1) {
                log.error(e1, "Commit error.");
            }
            try {
                writer.closeMem();
            } catch (IOException e1) {
                log.error(e1, "closeMem error.");
            }
            try {
                writer.deleteZK();
            } catch (IOException e1) {
                log.error(e1, "deleteZK error.");
            }
            try {
                writer.deleteHdfs();
            } catch (IOException e1) {
                log.error(e1, "deleteHdfs error.");
            }
            return null;
        }
        return writer;
    }

}
