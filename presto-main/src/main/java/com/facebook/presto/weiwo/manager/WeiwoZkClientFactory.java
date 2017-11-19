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
package com.facebook.presto.weiwo.manager;

import static java.util.Objects.requireNonNull;

import javax.inject.Inject;

import org.I0Itec.zkclient.ZkClient;

import com.facebook.presto.server.ServerConfig;

public class WeiwoZkClientFactory {

    private final String zkServers;
    private final int sessionTimeout;
    private final int connectionTimeout;
    private ZkClient client;

    @Inject
    public WeiwoZkClientFactory(ServerConfig config) {
        requireNonNull(config, "config is null");
        this.zkServers = requireNonNull(config.getZkServers(), "zkServers is null");
        this.sessionTimeout = requireNonNull(config.getSessionTimeout(), "sessionTimeout is null");
        this.connectionTimeout = requireNonNull(config.getConnectionTimeout(), "connectionTimeout is null");
    }

    public synchronized ZkClient getZkClient() {
        if (client == null) {
            client = new ZkClient(zkServers, sessionTimeout, connectionTimeout);
        }
        return client;
    }

}
