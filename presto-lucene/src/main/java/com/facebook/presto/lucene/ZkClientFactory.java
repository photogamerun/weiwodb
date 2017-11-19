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

import javax.inject.Inject;

import org.I0Itec.zkclient.ZkClient;

public class ZkClientFactory {

	private final String zkServers;
	private final int sessionTimeout;
	private final int connectionTimeout;
	private ZkClient client;

	@Inject
	public ZkClientFactory(LuceneConfig config) {
		requireNonNull(config, "config is null");
		zkServers = requireNonNull(config.getZkServers(), "zkServers is null");
		sessionTimeout = requireNonNull(config.getSessionTimeout(),
				"sessionTimeout is null");
		connectionTimeout = requireNonNull(config.getConnectionTimeout(),
				"connectionTimeout is null");
	}

	public synchronized ZkClient getZkClient() {
		if (client == null) {
			client = new ZkClient(zkServers, sessionTimeout, connectionTimeout);
		}
		return client;
	}

}
