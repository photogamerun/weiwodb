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
package com.facebook.presto.lucene.index;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.IndexReader;

import com.facebook.presto.lucene.hdfs.HdfsDirectory;

import io.airlift.log.Logger;

public class WeiwoIndexReader {

    private static final Logger log = Logger.get(WeiwoIndexReader.class);

    private IndexReader reader;
    private HdfsDirectory directory;
    private long totalBytes;
    private long initUsedBytes;
    private AtomicInteger currentRef;
    private String path;

    public WeiwoIndexReader(IndexReader reader, long totalBytes, long initUsedBytes, HdfsDirectory directory, String path) {
        this.reader = reader;
        this.totalBytes = totalBytes;
        this.initUsedBytes = initUsedBytes;
        this.directory = directory;
        currentRef = new AtomicInteger();
        this.path = path;
    }

    public IndexReader getReader() {
        currentRef.getAndIncrement();
        return reader;
    }

    public String getPath() {
        return path;
    }

    public HdfsDirectory getDirectory() {
        return directory;
    }

    public void returnReader(IndexReader reader) {
        currentRef.getAndDecrement();
    }
    
    public void decRef() {
        currentRef.getAndDecrement();
    }

    public long getTotalBytes() {
        return totalBytes;
    }
    
    public long getInitUsedBytes() {
        return initUsedBytes;
    }

    public int getCurrentRef() {
        return currentRef.get();
    }

    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            log.error(e, "Close IndexReader error");
        }
        try {
            directory.close();
        } catch (IOException e) {
            log.error(e, "Close Directory error");
        }
    }

}
