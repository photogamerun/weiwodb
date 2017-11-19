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

import static java.util.Objects.requireNonNull;

import com.facebook.presto.lucene.base.IndexInfo;
import com.google.common.base.Joiner;

public class IndexSplitKeyUtil {

	public static final String KEY_SEP = "_-_";

	public static IndexInfo getIndexInfoFromKey(String key) {
		requireNonNull(key, "key is null");
		String[] strs = key.split(KEY_SEP);
		String tt = null;
		if (strs.length != 5) {
			requireNonNull(tt, "Key Length is not 5");
		}
		return new IndexInfo(strs[0], strs[1], strs[2], strs[3], strs[4]);
	}

	public static String getIndexInfo(String db, String table, String partition,
			String id, String indexName) {
		requireNonNull(db, "db is null");
		requireNonNull(table, "table is null");
		requireNonNull(partition, "partition is null");
		requireNonNull(id, "id is null");
		requireNonNull(indexName, "indexName is null");
		return Joiner.on(KEY_SEP).join(db, table, partition, id, indexName);
	}

	public static String getIndexSplitKey(String id, String indexName) {
		requireNonNull(id, "id is null");
		requireNonNull(indexName, "indexName is null");
		return Joiner.on(KEY_SEP).join(id, indexName);
	}

	public static String getPartitionKey(String db, String table,
			String partition) {
		requireNonNull(db, "db is null");
		requireNonNull(table, "table is null");
		requireNonNull(partition, "partition is null");
		return Joiner.on(KEY_SEP).join(db, table, partition);
	}

}
