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
package com.facebook.presto.weiwo.data.source;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;

import com.alibaba.fastjson.JSON;
import com.facebook.presto.lucene.base.DataSource;
import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.weiwo.manager.WeiwoZkClientFactory;

import io.airlift.log.Logger;

public class WeiwoDataSourceCache {

    private static final Logger log = Logger.get(WeiwoDataSourceCache.class);

    final String sourcePath;
    final ServerConfig config;
    final WeiwoZkClientFactory zkClientFactory;
    final Map<String, DataSource> sources;

    @Inject
    public WeiwoDataSourceCache(WeiwoZkClientFactory zkClientFactory, ServerConfig config) {
        this.zkClientFactory = requireNonNull(zkClientFactory, "zkClientFactory is null");
        this.config = requireNonNull(config, "config is null");
        this.sourcePath = config.getZkPath() + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
                + WeiwoDBConfigureKeys.WEIWO_SOURCE_PATH;
        this.sources = new ConcurrentHashMap<String, DataSource>();
        init();
    }

    private void init() {
        if (!zkClientFactory.getZkClient().exists(sourcePath)) {
            zkClientFactory.getZkClient().createPersistent(sourcePath, true);
        }
        List<String> list = zkClientFactory.getZkClient().getChildren(sourcePath);
        if (list != null) {
            list.forEach(source -> {
                String data = zkClientFactory.getZkClient()
                        .readData(sourcePath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + source);
                DataSource ds = JSON.parseObject(data, DataSource.class);
                sources.put(source, ds);
            });
        }
    }

    public void addSource(DataSource source) throws IOException {
        if (sources.get(source.getName()) != null) {
            throw new IOException("Source exists.");
        }
        log.info("Add source. source = " + JSON.toJSONString(source));
        zkClientFactory.getZkClient().createPersistent(
                sourcePath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + source.getName(), JSON.toJSONString(source));
        sources.put(source.getName(), source);
    }

    public void deleteSource(String sourceName) throws IOException {
        if (sources.get(sourceName) == null) {
            log.warn("No source. source name = " + sourceName);
            return;
        }
        log.info("Delete source. name = " + sourceName);
        zkClientFactory.getZkClient().delete(sourcePath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + sourceName);
        sources.remove(sourceName);
    }

    public DataSource getSource(String sourceName) {
        return sources.get(sourceName);
    }

    public boolean existSource(String sourceName) {
        if (sources.get(sourceName) != null) {
            return true;
        } else {
            return false;
        }
    }

    public Set<String> getSourcesName() {
        return sources.keySet();
    }
    
    public Collection<DataSource> getSources() {
        return sources.values();
    }

}
