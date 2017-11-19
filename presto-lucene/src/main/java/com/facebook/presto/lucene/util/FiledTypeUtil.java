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

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;

public class FiledTypeUtil {
    
    public static FieldType STRING_FIELD_TYPE;
    
    static{
        FieldType type = new FieldType();
        type.setOmitNorms(true);
        type.setIndexOptions(IndexOptions.DOCS);
        type.setStored(true);
        type.setTokenized(false);
        type.setDocValuesType(DocValuesType.SORTED);
        type.freeze();
        STRING_FIELD_TYPE = type;
    }
    
    public static FieldType getStringFieldType(){
        return STRING_FIELD_TYPE;
    }

}
