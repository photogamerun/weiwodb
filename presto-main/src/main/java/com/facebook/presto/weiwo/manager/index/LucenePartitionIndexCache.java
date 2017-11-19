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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LucenePartitionIndexCache {

    final String partitonKey;
    final ConcurrentHashMap<String, IndexInfoWithNodeInfo> openIndexs;

    public LucenePartitionIndexCache(String partitonKey) {
        this.openIndexs = new ConcurrentHashMap<>();
        this.partitonKey = partitonKey;
    }

    public synchronized void open(String key, IndexInfoWithNodeInfo indexInfo) {
        IndexInfoWithNodeInfo indexiwn = openIndexs.get(key);
        if (indexiwn == null) {
            openIndexs.put(key, indexInfo);
        } else {
            indexiwn.addNode(indexInfo);
        }
    }

    public synchronized void close(String key, IndexInfoWithNodeInfo indexInfo) {
        IndexInfoWithNodeInfo indexiwn = openIndexs.get(key);
        if (indexiwn != null) {
            indexiwn.deleteNode(indexInfo);
            if(indexiwn.getNodesNum() < 1){
                openIndexs.remove(key);
            }
        }
    }
    
    public int getCacheSize(){
        return openIndexs.size();
    }
    
    public List<IndexInfoWithNodeInfo> getIndexList(){
        List<IndexInfoWithNodeInfo> result = new ArrayList<>();
        result.addAll(openIndexs.values());
        return result;
    }

}
