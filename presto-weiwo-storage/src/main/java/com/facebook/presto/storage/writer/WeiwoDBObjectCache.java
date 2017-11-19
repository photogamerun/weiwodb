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

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Object Cache
 * 使用弱引用,使得未gc的object可以复用,避免频繁创建对象
 * @author folin01.chen
 *
 * @param <K>
 * @param <V>
 */
public class WeiwoDBObjectCache<K, V> {

    private final Map<K, ConcurrentLinkedQueue<WeakReference<V>>> cache;

    public WeiwoDBObjectCache() {
        this.cache = new ConcurrentHashMap<>();
    }

    public void cacheObject(K k, V v) {
        ConcurrentLinkedQueue<WeakReference<V>> queue = null;
        if (cache.get(k) == null) {
            synchronized (cache) {
                if (cache.get(k) == null) {
                    queue = new ConcurrentLinkedQueue<>();
                    cache.put(k, queue);
                }
            }
        }
        queue = cache.get(k);
        queue.offer(new WeakReference<V>(v));
    }

    public V getObject(K key) {
        ConcurrentLinkedQueue<WeakReference<V>> queue = cache.get(key);
        if (queue == null) {
            return null;
        }
        WeakReference<V> ref = null;
        while ((ref = queue.poll()) != null) {
            return ref.get();
        }
        return null;
    }

    public void cleanAll() {
        synchronized (cache) {
            for (K k : cache.keySet()) {
                ConcurrentLinkedQueue<WeakReference<V>> queue = cache.get(k);
                queue.clear();
            }
        }
    }

}
