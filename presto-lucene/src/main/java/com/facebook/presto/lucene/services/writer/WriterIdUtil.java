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

import com.google.common.base.Joiner;

public class WriterIdUtil {

    public static final String ID_SEP = "___";

    public static String genarate(String nodeId, int number) {
        return Joiner.on(ID_SEP).join(nodeId, number);
    }

    public static String[] split(String id) {
        String[] result = new String[2];
        result[0] = id.substring(id.lastIndexOf(ID_SEP) + ID_SEP.length(), id.length());
        result[1] = id.substring(0, id.lastIndexOf(ID_SEP));
        return result;
    }

}
