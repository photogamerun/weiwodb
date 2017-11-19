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
package com.facebook.presto.lucene;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.lucene.wltea.analyzer.lucene.IKAnalyzer;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.TimeZoneKey;

public class LuceneBytesRefTest {
    
    @Test
    public void TestBytesRef1() throws IOException{
        RAMDirectory d = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(new IKAnalyzer(true));
        LogDocMergePolicy policy = new LogDocMergePolicy();
        policy.setMinMergeDocs(1);
        policy.setMergeFactor(2);
        policy.setNoCFSRatio(1.0);
        config.setMergePolicy(policy);
        config.setUseCompoundFile(true);
        config.setRAMBufferSizeMB(128);
        config.setMaxBufferedDocs(512000);
        IndexWriter writer = new IndexWriter(d, config);
        Document doc = new Document();
        Field field = new SortedDocValuesField("name", new BytesRef("唯品会会员"));
        doc.add(field);
        writer.addDocument(doc);
        writer.commit();
        IndexReader reader = DirectoryReader.open(d);
        System.out.println(new String(reader.leaves().get(0).reader().getSortedDocValues("name").get(0).bytes, Charset.forName("utf-8")));
        d.close();
    }
    
    @Test
    public void TestBytesRef2() throws IOException{
        RAMDirectory d = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(new IKAnalyzer(true));
        LogDocMergePolicy policy = new LogDocMergePolicy();
        policy.setMinMergeDocs(1);
        policy.setMergeFactor(2);
        policy.setNoCFSRatio(1.0);
        config.setMergePolicy(policy);
        config.setUseCompoundFile(true);
        config.setRAMBufferSizeMB(128);
        config.setMaxBufferedDocs(512000);
        IndexWriter writer = new IndexWriter(d, config);
        Document doc = new Document();
        Field field = new SortedDocValuesField("name", new BytesRef("唯品会会员".getBytes(Charset.forName("utf8"))));
        doc.add(field);
        writer.addDocument(doc);
        writer.commit();
        IndexReader reader = DirectoryReader.open(d);
        System.out.println(new String(reader.leaves().get(0).reader().getSortedDocValues("name").get(0).bytes, Charset.forName("utf-8")));
        d.close();
    }
    
    public static void main(String[] args) {
        JSONObject json = new JSONObject();
        json.put("time", "2016-01-01");
        long start = System.currentTimeMillis();
        for (int i = 0;i< 5;i++){
            System.out.println(json.getLong("time"));
        }
        System.out.println(System.currentTimeMillis()-start);
    }

}
