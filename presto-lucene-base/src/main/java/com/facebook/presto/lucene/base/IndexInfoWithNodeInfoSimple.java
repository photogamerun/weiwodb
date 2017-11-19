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
package com.facebook.presto.lucene.base;

import static java.util.Objects.requireNonNull;

import java.util.List;

import com.facebook.presto.lucene.base.util.StringUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

public class IndexInfoWithNodeInfoSimple {
    
    private final IndexInfo indexInfo;
    private final List<WeiwoDBNodeInfo> nodes;
    
    @JsonCreator
    public IndexInfoWithNodeInfoSimple(@JsonProperty("indexInfo") IndexInfo indexInfo,
            @JsonProperty("nodes") List<WeiwoDBNodeInfo> nodes){
        requireNonNull(indexInfo, "indexInfo is null");
        requireNonNull(nodes, "nodes is null");
        this.indexInfo = indexInfo;
        this.nodes = ImmutableList.copyOf(nodes);
    }

    @JsonProperty
    public IndexInfo getIndexInfo() {
        return indexInfo;
    }

    @JsonProperty
    public List<WeiwoDBNodeInfo> getNodes() {
        return nodes;
    }
    
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("IndexInfo:");
        sb.append(indexInfo.toString());
        sb.append(". Nodes:");
        sb.append(StringUtils.listToString(nodes, ","));
        return sb.toString();
    }

}
