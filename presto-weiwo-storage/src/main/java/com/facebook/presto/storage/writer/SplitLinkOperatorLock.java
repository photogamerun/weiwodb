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
package com.facebook.presto.storage.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.storage.writer.WeiwoDBSplitsLinkOperator.WriterKey;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitLinkOperatorLock {

    private static final Logger logger = LoggerFactory.getLogger(SplitLinkOperatorLock.class);
    private static List<WriterKey> lockKey = new ArrayList<>();

    public synchronized static boolean lock(WriterKey key) {
        if (lockKey.contains(key)) {
            return false;
        } else {
            lockKey.add(key);
            return true;
        }
    }

    public synchronized static boolean unLock(WriterKey key) {
        lockKey.remove(key);
        return true;
    }


    public synchronized static boolean lock(FileSystem fs, Path f) {
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.create(f,false);
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        }finally {
            IOUtils.closeQuietly(outputStream);
        }
        return false;
    }

    public synchronized static boolean unLock(FileSystem fs, Path f) {
        try {
            fs.delete(f,false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
}
