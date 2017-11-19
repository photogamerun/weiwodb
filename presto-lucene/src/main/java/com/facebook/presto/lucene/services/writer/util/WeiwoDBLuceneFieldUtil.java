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

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;

public class WeiwoDBLuceneFieldUtil {

	public static FieldType string_type = new FieldType();
	public static FieldType text_type = new FieldType();

	static {
		string_type.setOmitNorms(true);
		string_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
		string_type.setStored(true);
		string_type.setTokenized(false);
		string_type.setDocValuesType(DocValuesType.SORTED);
		string_type.freeze();

		text_type.setOmitNorms(true);
		text_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
		text_type.setStored(true);
		text_type.setTokenized(true);
		text_type.setDocValuesType(DocValuesType.SORTED);
		text_type.freeze();
	}

}
