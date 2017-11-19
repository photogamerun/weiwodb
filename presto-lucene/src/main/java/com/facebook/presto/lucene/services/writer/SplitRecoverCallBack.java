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
import java.util.concurrent.ScheduledExecutorService;

import io.airlift.log.Logger;

public class SplitRecoverCallBack implements CallBack {

    private static final Logger log = Logger.get(SplitRecoverCallBack.class);

    private WeiwoDBSplitWriterManager wmanager;
    private String db;
    private String table;
    private String partition;
    private String id;
    ScheduledExecutorService closeService;

    public SplitRecoverCallBack(WeiwoDBSplitWriterManager wmanager, String db, String table,
            String partition, String id, ScheduledExecutorService closeService) {
        this.wmanager = wmanager;
        this.db = db;
        this.table = table;
        this.partition = partition;
        this.id = id;
        this.closeService = closeService;
    }

    @Override
    public void call(boolean success) {
        if (success) {
            WeiwoDBSplitRecoverWriter writer = wmanager.getRecoverSplitWriterByKey(db, table, partition, id);
            writer.addSplitInfo();
            try {
                writer.close();
            } catch (IOException e) {
                log.error(e, "WeiwoDBSplitRecoverSuccessCallBack close error.");
            }
            try {
                writer.deleteZK();
            } catch (Exception e) {
                log.error(e, "WeiwoDBSplitRecoverSuccessCallBack deleteZK error.");
            }
            try {
                writer.getWal().delete();
            } catch (Exception e) {
                log.error(e, "WeiwoDBSplitRecoverSuccessCallBack wal delete error.");
            }
            try {
                writer.unRegisterWriter();
            } catch (Exception e) {
                log.error(e, "WeiwoDBSplitRecoverSuccessCallBack unRegisterWriter error.");
            }
            wmanager.completeRecover(db, table, partition, id);
        } else {
            WeiwoDBSplitRecoverWriter writer = wmanager.getRecoverSplitWriterByKey(db, table, partition, id);
            try {
                writer.deleteZK();
            } catch (Exception e) {
                log.error(e, "WeiwoDBSplitRecoverErrorCallBack deleteZK error.");
            }
            try {
                writer.deleteHdfs();
            } catch (Exception e) {
                log.error(e, "WeiwoDBSplitRecoverErrorCallBack wal delete error.");
            }
            try {
                writer.unRegisterWriter();
            } catch (Exception e) {
                log.error(e, "WeiwoDBSplitRecoverErrorCallBack unRegisterWriter error.");
            }
            try {
                writer.close();
            } catch (Exception e) {
                log.error(e, "WeiwoDBSplitRecoverErrorCallBack close error.");
            }
            try {
                writer.closeMem();
            } catch (Exception e) {
                log.error(e, "WeiwoDBSplitRecoverErrorCallBack closeMem error.");
            }
            wmanager.retryRecover(db, table, partition, id, 3);
        }
    }

}
