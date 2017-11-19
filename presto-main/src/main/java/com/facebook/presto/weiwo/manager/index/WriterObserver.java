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

import java.util.Arrays;
import java.util.Objects;
import java.util.Observable;
import java.util.Observer;

import com.facebook.presto.lucene.base.IndexInfo;
import com.facebook.presto.lucene.base.WeiwoDBNodeInfo;

public class WriterObserver implements Observer {

    LuceneIndexWriterManager writerManager;
    IndexInfo indexInfo;
    WeiwoDBNodeInfo nodeInfo;

    public WriterObserver(LuceneIndexWriterManager writerManager, IndexInfo indexInfo, WeiwoDBNodeInfo nodeInfo) {
        this.writerManager = writerManager;
        this.indexInfo = indexInfo;
        this.nodeInfo = nodeInfo;
    }

    @Override
    public void update(Observable o, Object arg) {
        writerManager.close(new IndexInfoWithNodeInfo(indexInfo, Arrays.asList(new WeiwoDBNodeInfo[] { nodeInfo })));
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexInfo, nodeInfo);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final WriterObserver other = (WriterObserver) obj;
        if (other.indexInfo == null || other.nodeInfo == null) {
            return false;
        }
        return (other.indexInfo.equals(this.indexInfo) && other.nodeInfo.equals(this.nodeInfo));
    }

}
