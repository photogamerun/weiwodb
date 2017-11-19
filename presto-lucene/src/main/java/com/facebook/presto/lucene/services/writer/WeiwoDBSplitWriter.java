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

import org.apache.lucene.index.IndexReader;

import com.facebook.presto.lucene.services.writer.exception.WriterCommitException;
import com.facebook.presto.lucene.services.writer.exception.WriterDeleteException;

public interface WeiwoDBSplitWriter {
    
    public void init() throws IOException;
    
    public void commit() throws WriterCommitException;
    
    public void deleteZK() throws WriterDeleteException;
    
    public void closeMem() throws IOException;
    
    public void close() throws IOException;
    
    public void deleteHdfs() throws WriterDeleteException;
    
    public IndexReader getReader() throws IOException;
    
    public void returnReader(IndexReader reader) throws IOException;
    
    public int getReaderRefCount();
    
    public boolean isClosed();
    
    public WeiwoDBSplitWal getWal();
    
    public long getTotalSize();

}
