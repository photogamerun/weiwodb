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

import java.util.Map;

import javax.inject.Inject;

import org.weakref.jmx.internal.guava.base.Joiner;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.lucene.LuceneConfig;
import com.facebook.presto.lucene.ZkClientFactory;
import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;

public class ZookeeperLuceneFieldFetcher {

    LuceneConfig config;
    ZkClientFactory zkClientFactory;

    @Inject
    public ZookeeperLuceneFieldFetcher(LuceneConfig config, ZkClientFactory zkClientFactory) {
        this.config = config;
        this.zkClientFactory = zkClientFactory;
    }

    public Map<String, String> getLuceneFields(String db, String table) {
    	String zkTablePath = Joiner.on(WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR).join(config.getZkPath(),WeiwoDBConfigureKeys.WEIWO_TABLE_PATH,
    			db,table);
        if(zkClientFactory.getZkClient().exists(zkTablePath)){
            String data = (String)zkClientFactory.getZkClient().readData(zkTablePath);
            JSONObject json = JSON.parseObject(data);
            Map<String, String> map = JSON.toJavaObject(json.getJSONObject(WeiwoDBConfigureKeys.WEIWO_TABLE_LUCENE_SCHEMA_KEY), Map.class);
            return map;
        } else {
            return null;
        }
    }
}
