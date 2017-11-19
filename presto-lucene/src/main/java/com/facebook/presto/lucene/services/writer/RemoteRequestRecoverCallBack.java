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

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.alibaba.fastjson.JSON;
import com.facebook.presto.lucene.LuceneConfig;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.log.Logger;

public class RemoteRequestRecoverCallBack implements CallBack {

    private static final Logger log = Logger.get(RemoteRequestRecoverCallBack.class);

    HttpClient httpClient;
    private WeiwoDBSplitWriterManager wmanager;
    private String db;
    private String table;
    private String partition;
    private String id;
    LuceneConfig config;

    public RemoteRequestRecoverCallBack(HttpClient httpClient, WeiwoDBSplitWriterManager wmanager, String db,
            String table, String partition, String id, LuceneConfig config) {
        this.httpClient = httpClient;
        this.wmanager = wmanager;
        this.db = db;
        this.table = table;
        this.partition = partition;
        this.id = id;
        this.config = config;
    }

    @Override
    public void call(boolean success) {
        if (success) {
            WeiwoDBSplitRecoverWriter writer = wmanager.getRecoverSplitWriterByKey(db, table, partition, id);
            writer.addSplitInfo();
            try {
                writer.close();
            } catch (IOException e) {
                log.error(e, "RemoteRequestRecoverCallBack close error.");
            }
            try {
                writer.deleteZK();
            } catch (Exception e) {
                log.error(e, "RemoteRequestRecoverCallBack deleteZK error.");
            }
            try {
                writer.getWal().delete();
            } catch (Exception e) {
                log.error(e, "RemoteRequestRecoverCallBack wal delete error.");
            }
            try {
                writer.unRegisterWriter();
            } catch (Exception e) {
                log.error(e, "RemoteRequestRecoverCallBack unRegisterWriter error.");
            }
            wmanager.completeRecover(db, table, partition, id);
            endRecover(writer.id, writer.indexName, success);
        } else {
            WeiwoDBSplitRecoverWriter writer = wmanager.getRecoverSplitWriterByKey(db, table, partition, id);
            try {
                writer.deleteZKAlive();
            } catch (Exception e) {
                log.error(e, "RemoteRequestRecoverCallBack deleteZKAlive error.");
            }
            try {
                writer.deleteHdfs();
            } catch (Exception e) {
                log.error(e, "RemoteRequestRecoverCallBack hdfs delete error.");
            }
            try {
                writer.unRegisterWriter();
            } catch (Exception e) {
                log.error(e, "RemoteRequestRecoverCallBack unRegisterWriter error.");
            }
            try {
                writer.close();
            } catch (Exception e) {
                log.error(e, "RemoteRequestRecoverCallBack close error.");
            }
            wmanager.completeRecover(db, table, partition, id);
            endRecover(writer.id, writer.indexName, success);
        }
    }

    private void endRecover(String nodeId, String indexName, boolean success) {
        HttpUriBuilder uriBuilder = uriBuilderFrom(URI.create(config.getWeiwomanagerUri()));
        Map<String, Object> map = new HashMap<>();
        map.put("nodeId", nodeId);
        map.put("indexName", indexName);
        map.put("success", success);
        uriBuilder.appendPath("/v1/recover/end");
        Request request = preparePost().setUri(uriBuilder.build())
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                .setBodyGenerator(
                        StaticBodyGenerator.createStaticBodyGenerator(JSON.toJSONString(map), Charset.forName("UTF-8")))
                .build();
        StatusResponse res = httpClient.execute(request, createStatusResponseHandler());
        if (res.getStatusCode() != Status.OK.getStatusCode()) {
            log.error("Send end remote recover error : " + res.getStatusMessage());
        } else {
            log.info("Send end remote recover sucess.");
        }
    }

}
