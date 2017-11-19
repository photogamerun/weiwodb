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
package com.facebook.presto.spi.pushdown;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PushDown {

	private Map<Class<?>, Object> pushDownPairs = new HashMap<Class<?>, Object>();

	public PushDown(Pair<?>... pair) {
		pushDownPairs = Stream.of(pair).filter(item -> item != null)
				.collect(Collectors.toMap(Pair::getClass, Pair::getValue));
	}

	public <V> V getValue(Class<?> key) {
		Type ty = ((ParameterizedType) key.getGenericInterfaces()[0])
				.getActualTypeArguments()[0];
		if (ty instanceof Class) {
			return ((Class<V>) ty).cast(pushDownPairs.get(key));
		} else {
			Class<V> o = (Class<V>) ((ParameterizedType) ty).getRawType();
			return o.cast(pushDownPairs.get(key));
		}
	}

	public void addPair(Pair pair) {
		pushDownPairs.put(pair.getClass(), pair.getValue());
	}

	public void fromPushDown(PushDown pushdown) {
		this.pushDownPairs = pushdown.pushDownPairs;
	}

	public boolean has(Class<?> key) {
		return pushDownPairs.get(key) != null;
	}

}