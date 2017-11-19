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
package com.facebook.presto.weiwo.manager.index;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.lucene.base.IndexInfo;
import com.facebook.presto.lucene.base.WeiwoDBNodeInfo;
import com.facebook.presto.lucene.base.util.IndexSplitKeyUtil;
import com.facebook.presto.lucene.base.util.StringUtils;

import io.airlift.log.Logger;

public class IndexInfoWithNodeInfo {

    private static final Logger log = Logger.get(IndexInfoWithNodeInfo.class);

    private IndexInfo indexInfo;
    private List<WeiwoDBNodeInfo> nodes;

    public IndexInfoWithNodeInfo(IndexInfo indexInfo, List<WeiwoDBNodeInfo> nodes) {
        this.indexInfo = indexInfo;
        this.nodes = new ArrayList<>();
        if (nodes != null) {
            this.nodes.addAll(nodes);
        }
    }

    public IndexInfo getIndexInfo() {
        return indexInfo;
    }

    public void setIndexInfo(IndexInfo indexInfo) {
        this.indexInfo = indexInfo;
    }

    public List<WeiwoDBNodeInfo> getNodes() {
        ArrayList<WeiwoDBNodeInfo> result = new ArrayList<>();
        result.addAll(nodes);
        return result;
    }

    public int getNodesNum() {
        return nodes.size();
    }

    public synchronized void addNode(IndexInfoWithNodeInfo indexInfoWithNode) {
        requireNonNull(indexInfoWithNode, "indexInfoWithNode is null");
        requireNonNull(indexInfoWithNode.getIndexInfo(), "indexInfo is null");
        requireNonNull(indexInfoWithNode.getNodes(), "nodes is null");
        if (indexInfoWithNode.getNodes().size() < 1) {
            return;
        }
        if (indexInfoWithNode.getNodes().size() > 1) {
            log.error("Add node for single index , More than one node be added to this index:"
                    + IndexSplitKeyUtil.getIndexInfo(indexInfoWithNode.getIndexInfo().getSchema(),
                            indexInfoWithNode.getIndexInfo().getTable(),
                            indexInfoWithNode.getIndexInfo().getPartition(), indexInfoWithNode.getIndexInfo().getId(),
                            indexInfoWithNode.getIndexInfo().getIndexName())
                    + ". Nodes :" + StringUtils.listToString(indexInfoWithNode.getNodes(), ","));
        }
        if (!nodes.contains(indexInfoWithNode.getNodes().get(0))) {
            nodes.add(indexInfoWithNode.getNodes().get(0));
        }
    }

    public synchronized void deleteNode(IndexInfoWithNodeInfo indexInfoWithNode) {
        requireNonNull(indexInfoWithNode, "indexInfoWithNode is null");
        requireNonNull(indexInfoWithNode.getIndexInfo(), "indexInfo is null");
        requireNonNull(indexInfoWithNode.getNodes(), "nodes is null");
        if (indexInfoWithNode.getNodes().size() < 1) {
            return;
        }
        if (indexInfoWithNode.getNodes().size() > 1) {
            log.error("Delete node for single index , More than one node be delete to this index:"
                    + IndexSplitKeyUtil.getIndexInfo(indexInfoWithNode.getIndexInfo().getSchema(),
                            indexInfoWithNode.getIndexInfo().getTable(),
                            indexInfoWithNode.getIndexInfo().getPartition(), indexInfoWithNode.getIndexInfo().getId(),
                            indexInfoWithNode.getIndexInfo().getIndexName())
                    + ". Nodes :" + StringUtils.listToString(indexInfoWithNode.getNodes(), ","));
        }
        if (nodes.contains(indexInfoWithNode.getNodes().get(0))) {
            nodes.remove(indexInfoWithNode.getNodes().get(0));
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IndexInfo:");
        sb.append(indexInfo.toString());
        sb.append(". Nodes:");
        sb.append(StringUtils.listToString(nodes, ","));
        return sb.toString();
    }

}
