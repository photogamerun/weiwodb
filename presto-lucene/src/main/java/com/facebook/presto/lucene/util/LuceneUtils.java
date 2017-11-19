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

import java.util.HashMap;
import java.util.Map;

public class LuceneUtils {

	public static String FILE_SEP = "/";

	public final static String WEIWODB_SCHEMA_MAPPING_KEY = "weiwodb.schema.mapping";
	public final static String WEIWODB_DATA_LOCATION = "weiwodb.data.location";
	public final static String WEIWODB_STORAGE_HANDLER = "storage_handler";

	public static String getSchemaMapping(Map<String, String> map) {
		if (map != null) {
			return map.get(WEIWODB_SCHEMA_MAPPING_KEY);
		}
		return null;
	}

	public static Map<String, String> getLuceneColumnTypes(
			Map<String, String> map) {
		String schema = getSchemaMapping(map);
		Map<String, String> columns = new HashMap<>();
		if (schema != null && !"".equals(schema)) {
			String[] pairs = schema.trim().split(",");
			if (pairs != null) {
				for (String pair : pairs) {
					if (pair != null && !"".equals(pair)) {
						String[] result = pair.trim().split("\\s+");
						if (result != null && result.length == 2) {
							if (result[0] != null && !"".equals(result[0])
									&& result[1] != null
									&& !"".equals(result[1])) {
								columns.put(result[0], result[1]);
							}
						}
					}
				}
			}
		}
		return columns;
	}

	public static Map<String, String> getLuceneColumnTypes(String mapping) {
		Map<String, String> columns = new HashMap<>();
		if (mapping != null && !"".equals(mapping)) {
			String[] pairs = mapping.trim().split(",");
			if (pairs != null) {
				for (String pair : pairs) {
					if (pair != null && !"".equals(pair)) {
						String[] result = pair.trim().split("\\s+");
						if (result != null && result.length == 2) {
							if (result[0] != null && !"".equals(result[0])
									&& result[1] != null
									&& !"".equals(result[1])) {
								columns.put(result[0], result[1]);
							}
						}
					}
				}
			}
		}
		return columns;
	}

	public static String getLuceneColumnType(Map<String, String> map,
			String col) {
		if (col == null || "".equals(col)) {
			return null;
		}
		Map<String, String> columns = getLuceneColumnTypes(map);
		if (columns != null) {
			return columns.get(col);
		}
		return null;
	}
}
