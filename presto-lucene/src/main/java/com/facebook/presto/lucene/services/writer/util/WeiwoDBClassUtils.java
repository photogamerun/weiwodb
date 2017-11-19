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
package com.facebook.presto.lucene.services.writer.util;

import java.util.HashMap;
import java.util.Map;

import com.facebook.presto.lucene.services.writer.WeiwoDBDataWriter;

public class WeiwoDBClassUtils {

    public static Map<String, Class<?>> cache = new HashMap<>();

    public static synchronized Class<?> getClassByNameOrNull(String className) {
        Class<?> clazz = null;
        Class<?> ref = cache.get(className);
        if (ref != null) {
            clazz = ref;
        }

        if (clazz == null) {
            try {
                clazz = Class.forName(className);
            } catch (ClassNotFoundException e) {
                cache.remove(className);
                return null;
            }
            cache.put(className, clazz);
            return clazz;
        } else {
            return clazz;
        }
    }

    public static <T> T newInstance(Class<T> theClass) throws Exception {
        T result = theClass.newInstance();
        if (result instanceof WeiwoDBDataWriter) {
            return result;
        } else {
            return null;
        }
    }

}
