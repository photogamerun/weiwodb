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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;

import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;
import com.facebook.presto.lucene.hdfs.HdfsDirectory;
import com.facebook.presto.lucene.services.writer.util.IKAnalyzerUtil;

public class ReFlushIndex {

    public static void main(String[] args) throws IOException {
        String path = args[0];
        String pathnew = path + ".new";
        FileSystem fs = FileSystem.get(new HdfsConfiguration());
        fs.rename(new Path(path), new Path(pathnew));
        HdfsDirectory in = new HdfsDirectory("test", new Path(pathnew), new HdfsConfiguration(), fs, true);
        HdfsDirectory out = new HdfsDirectory("test1", new Path(path), new HdfsConfiguration(), fs);
        IndexWriterConfig luceneConfig = createConfig();
        IndexWriter writer = new IndexWriter(out, luceneConfig);
        List<SegmentCommitInfo> list = new ArrayList<>();
        list.addAll(SegmentInfos.readLatestCommit(in).asList());
        writer.merge(new OneMerge(list));
        try {
            writer.close();
        } catch (IOException e) {

        }
        fs.delete(new Path(path + "/write.lock"));
        in.close();
        out.close();
        fs.delete(new Path(pathnew));
        fs.close();
    }

    private static IndexWriterConfig createConfig() {
        IndexWriterConfig luceneConfig = new IndexWriterConfig(IKAnalyzerUtil.getAnalyzer());
        LogDocMergePolicy policy = new LogDocMergePolicy();
        policy.setMinMergeDocs(1);
        policy.setMergeFactor(2);
        policy.setNoCFSRatio(1.0);
        luceneConfig.setMergePolicy(policy);
        luceneConfig.setUseCompoundFile(true);
        luceneConfig.setRAMBufferSizeMB(WeiwoDBConfigureKeys.RAM_BUFFER_SIZE_MB);
        luceneConfig.setMaxBufferedDocs(WeiwoDBConfigureKeys.MAX_BUFFER_DOCS);
        return luceneConfig;
    }

}
