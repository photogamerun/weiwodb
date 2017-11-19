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
package com.facebook.presto.lucene.base.util;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class StringUtils {
    
    public static String listToString(Set<? extends Object> list, String separator) {

        if (list == null) {
            return "[]";
        }
        
        Iterator<? extends Object> iterator = list.iterator();
        
        if (!iterator.hasNext()) {
            return "[]";
        }
        Object first = iterator.next();
        if (!iterator.hasNext()) {
            if (first == null) {
                return "[]";
            } else {
                return "[" + first.toString() + "]";
            }
        }

        StringBuilder buf = new StringBuilder();
        buf.append("[");
        if (first != null) {
            buf.append(first);
        }

        while (iterator.hasNext()) {
            if (separator != null) {
                buf.append(separator);
            }
            Object obj = iterator.next();
            if (obj != null) {
                buf.append(obj);
            }
        }
        buf.append("]");
        return buf.toString();
    }

    public static String listToString(List<? extends Object> list, String separator) {

        if (list == null) {
            return "[]";
        }
        
        Iterator<? extends Object> iterator = list.iterator();
        
        if (!iterator.hasNext()) {
            return "[]";
        }
        Object first = iterator.next();
        if (!iterator.hasNext()) {
            if (first == null) {
                return "[]";
            } else {
                return "[" + first.toString() + "]";
            }
        }

        StringBuilder buf = new StringBuilder();
        buf.append("[");
        if (first != null) {
            buf.append(first);
        }

        while (iterator.hasNext()) {
            if (separator != null) {
                buf.append(separator);
            }
            Object obj = iterator.next();
            if (obj != null) {
                buf.append(obj);
            }
        }
        buf.append("]");
        return buf.toString();
    }

}
