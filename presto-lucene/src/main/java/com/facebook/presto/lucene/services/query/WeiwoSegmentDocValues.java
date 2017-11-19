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

import io.airlift.log.Logger;

public abstract class WeiwoSegmentDocValues<T, V, X> {
	private static final Logger log = Logger.get(WeiwoSegmentDocValues.class);

	protected T docValues;

	protected V fieldValue;

	protected HashFunction hashFunction;

	protected boolean isExisted;

	protected Field field;

	protected Integer channel;

	public WeiwoSegmentDocValues(T docValues, HashFunction hashFunction,
			Field field, Integer channel) {
		this.docValues = docValues;
		log.info(" Thread "+Thread.currentThread()+ " output docValue " + docValues.hashCode());
		this.hashFunction = hashFunction;
		this.field = field;
		this.channel = channel;
	}

	protected abstract V getDocGroupValue(int doc);

	public Object[] write(Object[] row) {
		row[channel] = fieldValue;
		return row;
	}

	public long hash(int doc) {
		isExisted = false;
		fieldValue = getDocGroupValue(doc);
		if (fieldValue == null) {
			return -7286425919675154353l;
		} else {
			return hashFunction.hash(fieldValue);
		}
	}

	public boolean write(Object[] row, int docid) {
		return false;
	}

	public boolean isExisted() {
		return isExisted;
	}

	public static class Field {

		private String columnName;

		private boolean isDistinct;

		public Field(String columnName, boolean isDistinct) {
			this.columnName = columnName;
			this.isDistinct = isDistinct;
		}

		public String getColumnName() {
			return columnName;
		}

		public boolean isDistinct() {
			return isDistinct;
		}
	}

	public Field getField() {
		return field;
	}
}