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
package com.facebook.presto.lucene.query;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.facebook.presto.lucene.base.DataSource;
import com.facebook.presto.lucene.base.DataSourceType;

public class TestKakfaSource {
    
    public static void main(String[] args) {
        DataSource source = new DataSource();
        source.setAllowConcurrentPerNode(false);
        source.setType(DataSourceType.KAFKA);
        source.setName("weiwo");
        source.setNodes(2);
        source.setClassName("com.facebook.presto.lucene.writer.kafka.KafkaDataSourceWriter");
        Map<String, Object> properties = new HashMap<>();
        properties.put("zookeeper.connect","y153-hadoop-namenode2.vclound.com,y154-hadoop-datanode.vclound.com,y202-hadoop-datanode.vclound.com:2181");
        properties.put("group.id", "weiwo");
        properties.put("kafka.topic.name","weiwo");
        properties.put("zookeeper.session.timeout.ms",30000);
        properties.put("zookeeper.sync.time.ms",400);
        properties.put("auto.commit.interval.ms",2000);
        properties.put("consumer.thread.num",1);
        source.setProperties(properties);
        System.out.println(JSON.toJSONString(source));;
    }

}
