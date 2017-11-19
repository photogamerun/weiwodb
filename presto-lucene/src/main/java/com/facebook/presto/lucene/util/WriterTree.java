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
package com.facebook.presto.lucene.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 采用tree的方式存储object,比如writer
 * 
 * @author folin01.chen
 *
 */
@SuppressWarnings("rawtypes")
public class WriterTree<K extends Comparable<K>, V> implements Comparable<K> {

    public K k;
    public V v;
    public List<WriterTree<K, V>> child;

    public WriterTree(K k) {
        this.k = k;
        this.child = new ArrayList<>();
    }

    public WriterTree(K k, V v) {
        this.k = k;
        this.v = v;
        this.child = new ArrayList<>();
    }
    
    public V get(@SuppressWarnings("unchecked") K... keys) {
        if (keys != null) {
            if (keys.length == 0) {
                return v;
            } else if (keys.length == 1) {
                K key = keys[0];
                int index = Collections.binarySearch(child, key);
                if (index >= 0) {
                    return child.get(index).v;
                } else {
                    return null;
                }
            } else {
                K key = keys[0];
                int index = Collections.binarySearch(child, key);
                if (index >= 0) {
                    WriterTree<K, V> value = child.get(index);
                    return value.get(Arrays.copyOfRange(keys, 1, keys.length));
                } else {
                    return null;
                }
            }
        } else {
            return null;
        }
    }
    
    public V remove(@SuppressWarnings("unchecked") K... keys) {
        if (keys != null) {
            if (keys.length == 0) {
                return v;
            } else if (keys.length == 1) {
                K key = keys[0];
                int index = Collections.binarySearch(child, key);
                if (index >= 0) {
                    return child.remove(index).v;
                } else {
                    return null;
                }
            } else {
                K key = keys[0];
                int index = Collections.binarySearch(child, key);
                if (index >= 0) {
                    WriterTree<K, V> value = child.get(index);
                    V v = value.remove(Arrays.copyOfRange(keys, 1, keys.length));
                    if(value.size() == 0){
                        child.remove(index);
                    }
                    return v;
                } else {
                    return null;
                }
            }
        } else {
            return null;
        }
    }

    public void add(V v, @SuppressWarnings("unchecked") K... keys) {
        if (keys != null) {
            if (keys.length == 0) {
            } else if (keys.length == 1) {
                K key = keys[0];
                int index = Collections.binarySearch(child, key);
                if (index >= 0) {
                    WriterTree<K, V> value = child.get(index);
                    value.setValue(v);
                } else {
                    child.add(-index - 1, new WriterTree<K, V>(key, v));
                }
            } else {
                K key = keys[0];
                int index = Collections.binarySearch(child, key);
                WriterTree<K, V> value = null;
                if (index >= 0) {
                    value = child.get(index);
                } else {
                    value = new WriterTree<K, V>(key);
                    child.add(-index - 1, value);
                }
                value.add(v, Arrays.copyOfRange(keys, 1, keys.length));
            }
        }
    }
    
    public void setValue(V v){
        this.v = v;
    }
    
    public int size(){
        return child.size();
    }

    @Override
    public int compareTo(K o) {
        return this.k.compareTo(o);
    }

}
