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

import java.util.List;

import com.facebook.presto.lucene.base.HeartBeatCommand;
import com.facebook.presto.lucene.base.HostDataSources;
import com.facebook.presto.lucene.base.IndexInfoWithNodeInfoSimple;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;

public class JsonCodecUtils {

    public static final JsonCodec<List<IndexInfoWithNodeInfoSimple>> INDEX_INFO_LIST_CODEC;
    public static final JsonCodec<IndexInfoWithNodeInfoSimple> INDEX_INFO_CODEC;
    public static final JsonCodec<HeartBeatCommand> HEART_BEAT_COMMAND_CODEC;
    public static final JsonCodec<HostDataSources> HOST_DATA_SOURCES_CODEC;

    static {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        INDEX_INFO_CODEC = new JsonCodecFactory(provider).jsonCodec(IndexInfoWithNodeInfoSimple.class);
        INDEX_INFO_LIST_CODEC = new JsonCodecFactory(provider).listJsonCodec(IndexInfoWithNodeInfoSimple.class);
        HEART_BEAT_COMMAND_CODEC = new JsonCodecFactory(provider).jsonCodec(HeartBeatCommand.class);
        HOST_DATA_SOURCES_CODEC = new JsonCodecFactory(provider).jsonCodec(HostDataSources.class);
    }

    public static JsonCodec<List<IndexInfoWithNodeInfoSimple>> getIndexInfoListCodec() {
        return INDEX_INFO_LIST_CODEC;
    }

    public static JsonCodec<IndexInfoWithNodeInfoSimple> getIndexInfoCodec() {
        return INDEX_INFO_CODEC;
    }
    
    public static JsonCodec<HeartBeatCommand> getHeartBeatCommandCodec() {
        return HEART_BEAT_COMMAND_CODEC;
    }
    
    public static JsonCodec<HostDataSources> getHostDataSourcesCodec() {
        return HOST_DATA_SOURCES_CODEC;
    }

}
