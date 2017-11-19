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
package com.googlecode.concurrentlinkedhashmap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A common set of {@link Weigher} implementations.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @see <a href="http://code.google.com/p/concurrentlinkedhashmap/">
 *      http://code.google.com/p/concurrentlinkedhashmap/</a>
 */
public final class CacheWeighers {

  private CacheWeighers() {
    throw new AssertionError();
  }

  /**
   * A weigher where a value has a weight of <tt>1</tt>. A map bounded with
   * this weigher will evict when the number of key-value pairs exceeds the
   * capacity.
   *
   * @return A weigher where a value takes one unit of capacity.
   */
  @SuppressWarnings({"cast", "unchecked"})
  public static <V> CacheWeigher<V> singleton() {
    return (CacheWeigher<V>) SingletonWeigher.INSTANCE;
  }

  /**
   * A weigher where the value is a byte array and its weight is the number of
   * bytes. A map bounded with this weigher will evict when the number of bytes
   * exceeds the capacity rather than the number of key-value pairs in the map.
   * This allows for restricting the capacity based on the memory-consumption
   * and is primarily for usage by dedicated caching servers that hold the
   * serialized data.
   * <p>
   * A value with a weight of <tt>0</tt> will be rejected by the map. If a value
   * with this weight can occur then the caller should eagerly evaluate the
   * value and treat it as a removal operation. Alternatively, a custom weigher
   * may be specified on the map to assign an empty value a positive weight.
   *
   * @return A weigher where each byte takes one unit of capacity.
   */
  public static CacheWeigher<byte[]> byteArray() {
    return ByteArrayWeigher.INSTANCE;
  }

  /**
   * A weigher where the value is a {@link Iterable} and its weight is the
   * number of elements. This weigher only should be used when the alternative
   * {@link #collection()} weigher cannot be, as evaluation takes O(n) time. A
   * map bounded with this weigher will evict when the total number of elements
   * exceeds the capacity rather than the number of key-value pairs in the map.
   * <p>
   * A value with a weight of <tt>0</tt> will be rejected by the map. If a value
   * with this weight can occur then the caller should eagerly evaluate the
   * value and treat it as a removal operation. Alternatively, a custom weigher
   * may be specified on the map to assign an empty value a positive weight.
   *
   * @return A weigher where each element takes one unit of capacity.
   */
  @SuppressWarnings({"cast", "unchecked"})
  public static <E> CacheWeigher<? super Iterable<E>> iterable() {
    return (CacheWeigher<Iterable<E>>) (CacheWeigher<?>) IterableWeigher.INSTANCE;
  }

  /**
   * A weigher where the value is a {@link Collection} and its weight is the
   * number of elements. A map bounded with this weigher will evict when the
   * total number of elements exceeds the capacity rather than the number of
   * key-value pairs in the map.
   * <p>
   * A value with a weight of <tt>0</tt> will be rejected by the map. If a value
   * with this weight can occur then the caller should eagerly evaluate the
   * value and treat it as a removal operation. Alternatively, a custom weigher
   * may be specified on the map to assign an empty value a positive weight.
   *
   * @return A weigher where each element takes one unit of capacity.
   */
  @SuppressWarnings({"cast", "unchecked"})
  public static <E> CacheWeigher<? super Collection<E>> collection() {
    return (CacheWeigher<Collection<E>>) (CacheWeigher<?>) CollectionWeigher.INSTANCE;
  }

  /**
   * A weigher where the value is a {@link List} and its weight is the number
   * of elements. A map bounded with this weigher will evict when the total
   * number of elements exceeds the capacity rather than the number of
   * key-value pairs in the map.
   * <p>
   * A value with a weight of <tt>0</tt> will be rejected by the map. If a value
   * with this weight can occur then the caller should eagerly evaluate the
   * value and treat it as a removal operation. Alternatively, a custom weigher
   * may be specified on the map to assign an empty value a positive weight.
   *
   * @return A weigher where each element takes one unit of capacity.
   */
  @SuppressWarnings({"cast", "unchecked"})
  public static <E> CacheWeigher<? super List<E>> list() {
    return (CacheWeigher<List<E>>) (CacheWeigher<?>) ListWeigher.INSTANCE;
  }

  /**
   * A weigher where the value is a {@link Set} and its weight is the number
   * of elements. A map bounded with this weigher will evict when the total
   * number of elements exceeds the capacity rather than the number of
   * key-value pairs in the map.
   * <p>
   * A value with a weight of <tt>0</tt> will be rejected by the map. If a value
   * with this weight can occur then the caller should eagerly evaluate the
   * value and treat it as a removal operation. Alternatively, a custom weigher
   * may be specified on the map to assign an empty value a positive weight.
   *
   * @return A weigher where each element takes one unit of capacity.
   */
  @SuppressWarnings({"cast", "unchecked"})
  public static <E> CacheWeigher<? super Set<E>> set() {
    return (CacheWeigher<Set<E>>) (CacheWeigher<?>) SetWeigher.INSTANCE;
  }

  /**
   * A weigher where the value is a {@link Map} and its weight is the number of
   * entries. A map bounded with this weigher will evict when the total number of
   * entries across all values exceeds the capacity rather than the number of
   * key-value pairs in the map.
   * <p>
   * A value with a weight of <tt>0</tt> will be rejected by the map. If a value
   * with this weight can occur then the caller should eagerly evaluate the
   * value and treat it as a removal operation. Alternatively, a custom weigher
   * may be specified on the map to assign an empty value a positive weight.
   *
   * @return A weigher where each entry takes one unit of capacity.
   */
  @SuppressWarnings({"cast", "unchecked"})
  public static <A, B> CacheWeigher<? super Map<A, B>> map() {
    return (CacheWeigher<Map<A, B>>) (CacheWeigher<?>) MapWeigher.INSTANCE;
  }

  private enum SingletonWeigher implements CacheWeigher<Object> {
    INSTANCE;

    @Override
    public long weightOf(Object value) {
      return 1;
    }
  }

  private enum ByteArrayWeigher implements CacheWeigher<byte[]> {
    INSTANCE;

    @Override
    public long weightOf(byte[] value) {
      return value.length;
    }
  }

  private enum IterableWeigher implements CacheWeigher<Iterable<?>> {
    INSTANCE;

    @Override
    public long weightOf(Iterable<?> values) {
      if (values instanceof Collection<?>) {
        return ((Collection<?>) values).size();
      }
      int size = 0;
      for (Object value : values) {
        size++;
      }
      return size;
    }
  }

  private enum CollectionWeigher implements CacheWeigher<Collection<?>> {
    INSTANCE;

    @Override
    public long weightOf(Collection<?> values) {
      return values.size();
    }
  }

  private enum ListWeigher implements CacheWeigher<List<?>> {
    INSTANCE;

    @Override
    public long weightOf(List<?> values) {
      return values.size();
    }
  }

  private enum SetWeigher implements CacheWeigher<Set<?>> {
    INSTANCE;

    @Override
    public long weightOf(Set<?> values) {
      return values.size();
    }
  }

  private enum MapWeigher implements CacheWeigher<Map<?, ?>> {
    INSTANCE;

    @Override
    public long weightOf(Map<?, ?> values) {
      return values.size();
    }
  }
}
