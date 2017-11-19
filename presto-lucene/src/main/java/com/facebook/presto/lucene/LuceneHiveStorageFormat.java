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
package com.facebook.presto.lucene;

import static java.util.Objects.requireNonNull;

import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;

public enum LuceneHiveStorageFormat {

    WEIWO(LazySimpleSerDe.class.getName(),
            "org.apache.hadoop.hive.ql.io.HiveInputFormat",
            "com.facebook.presto.storage.WeiwoOutputFormat"),
    SEQUENCEFILE(LazySimpleSerDe.class.getName(),
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),
    TEXTFILE(LazySimpleSerDe.class.getName(),
            "org.apache.hadoop.mapred.TextInputFormat",
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    
    private final String serde;
    private final String inputFormat;
    private final String outputFormat;

    LuceneHiveStorageFormat(String serde, String inputFormat, String outputFormat)
    {
        this.serde = requireNonNull(serde, "serde is null");
        this.inputFormat = requireNonNull(inputFormat, "inputFormat is null");
        this.outputFormat = requireNonNull(outputFormat, "outputFormat is null");
    }

    public String getSerDe()
    {
        return serde;
    }

    public String getInputFormat()
    {
        return inputFormat;
    }

    public String getOutputFormat()
    {
        return outputFormat;
    }

}
