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

import java.util.ArrayList;
import java.util.List;

import com.facebook.presto.lucene.services.writer.WeiwoDBSplitWriterManager.WriterKey;

public class SplitLinkOperatorLock {

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
}
