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

public class WeiwoDBType {

	public final static String INT = "int";
	public final static String LONG = "long";
	public final static String DOUBLE = "double";
	public final static String FLOAT = "float";
	public final static String STRING = "string";
	public final static String TIMESTAMP = "timestamp";
	public final static String TEXT = "text";
	public final static String UNKNOWN = "unknown";

	public static String getType(String type) {
		if (INT.equalsIgnoreCase(type)) {
			return INT;
		} else if (LONG.equalsIgnoreCase(type)) {
			return LONG;
		} else if (DOUBLE.equalsIgnoreCase(type)) {
			return DOUBLE;
		} else if (STRING.equalsIgnoreCase(type)) {
			return STRING;
		} else if (TIMESTAMP.equalsIgnoreCase(type)) {
			return TIMESTAMP;
		} else if (FLOAT.equalsIgnoreCase(type)) {
			return FLOAT;
		} else if (TEXT.equalsIgnoreCase(type)) {
			return TEXT;
		} else {
			return UNKNOWN;
		}
	}

}
