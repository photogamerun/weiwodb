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
package com.facebook.presto.storage;

public class WeiwoDBConfigureKeys {

    public static final String ZK_PATH_SEPARATOR = "/";
    public static final String HDFS_PATH_SEPARATOR = "/";
    public static final String WEIWO_PIPE_IMPL = "weiwo.pipe.impl";
    public static final String INDEX_NAME_PREFIX = "weiwo_";
    public static final String WEIWO_TABLE_PATH = "weiwo";
    public static final String WEIWO_TABLE_LUCENE_SCHEMA_KEY = "luceneschema";
    public static final String WEIWO_SOURCE_PATH = "source";
    public static final String WEIWO_WRITER_PATH = "writer";
    public static final String WEIWO_WRITER_WRITING_PATH = "writing";
    public static final String WEIWO_WRITER_ALIVE_PATH = "alive";
    public static final String WEIWO_WAL_FILE = "weiwo.wal";
    public static final String WEIWO_WAL_DIRECTORY = "WAL";
    public static final String WEIWO_SPLITS_FILE = "weiwo.splits";
    public static final String WEIWO_SPLITS_FILE_TMP = ".tmp";
    public static final int RAM_BUFFER_SIZE_MB = 1024;
    public static final int MAX_BUFFER_DOCS = Integer.MAX_VALUE;
    public static final int READER_OPEN_DELAY = 10;
    
    public static final String NUMERIC_KEY = "_-_n";
    public static final String SORTED_KEY = "_-_s";
    
    public static final String URI_PREFIX = "http://";
    
    public static final long CLOSE_DELAY_TIME = 300;
    
    public static final String JSON_DATAS_KEY = "datas";
    public static final String JSON_DB_KEY = "db";
    public static final String JSON_TABLE_KEY = "table";
    public static final String JSON_PARTITION_KEY = "partition";
    
    public static final String PROJECT_PATH = "project.path";

}
