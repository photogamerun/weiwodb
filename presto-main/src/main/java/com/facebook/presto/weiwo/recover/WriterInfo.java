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
package com.facebook.presto.weiwo.recover;

import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.facebook.presto.spi.Node;

public class WriterInfo {

    private String indexName;
    private Map<String, Object> properties;
    private long startTime;
    private Node recoverNode;

    public WriterInfo(String indexName, Map<String, Object> properties, long startTime, Node recoverNode) {
        this.indexName = indexName;
        this.properties = properties;
        this.startTime = startTime;
        this.recoverNode = recoverNode;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public Node getRecoverNode() {
        return recoverNode;
    }

    public void setRecoverNode(Node recoverNode) {
        this.recoverNode = recoverNode;
    }
    
    public String toString(){
        return JSON.toJSONString(this);
    }

}
