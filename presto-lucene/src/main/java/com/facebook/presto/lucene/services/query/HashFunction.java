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
package com.facebook.presto.lucene.services.query;

import org.apache.lucene.util.BytesRef;

import com.facebook.presto.lucene.services.query.util.CityHash;
/**
 * 
 * 
 * @author peter.wei
 *
 * @param <V>
 */
public interface HashFunction<V> {
	public abstract long hash(V v);

	public class LongHashFunction implements HashFunction<Long> {

		byte[] b = new byte[8];

		private boolean isMutiple;

		public LongHashFunction(boolean isMutiple) {
			this.isMutiple = isMutiple;
		}

		/**
		 * 如果是 Mutiple 意味着long的hash需要做combine hash, 这个时候就不能直接返回long的原值
		 * 很容易出现hash冲突,于是使用long的byte[]计算long 的hash 值. 如果不是combine
		 * 的hash计算则可以直接返回long原值。
		 * 
		 * @param v
		 * @return
		 */
		@Override
		public long hash(Long v) {
			if (isMutiple) {
				byte[] by = longToByte(v);
				return CityHash.cityHash64(by, 0, by.length);
			} else {
				return v;
			}
		}

		public byte[] longToByte(long number) {
			for (int i = 0; i < b.length; i++) {
				b[i] = Long.valueOf(number & 0xff).byteValue();
				number = number >> 8;
			}
			return b;
		}
	}

	public class VarcharHashFunction implements HashFunction<BytesRef> {

		@Override
		public long hash(BytesRef v) {
			if (v == null) {
				return -7286425919675154353l;
			} else {
				long nanocalHash = System.nanoTime();
				long hash = CityHash.cityHash64(v.bytes, v.offset, v.length);
				return hash;
			}
		}
	}

	public class TextHashFunction implements HashFunction<String> {

		@Override
		public long hash(String v) {
			if (v == null) {
				return -7286425919675154353l;
			}
			byte[] btOfString = v.getBytes();
			return CityHash.cityHash64(btOfString, 0, btOfString.length);
		}
	}

}
