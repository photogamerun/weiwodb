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
package com.facebook.presto.lucene.services.writer.internals;

import java.util.List;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;

import com.facebook.presto.lucene.Resource;

import io.airlift.log.Logger;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafKaConnectorResource implements Resource<ConsumerConnector> {

	private static final Logger log = Logger.get(KafKaConnectorResource.class);
	private Properties properties;
	private ConsumerConnector connector;
	private ZkClient zkClient;

	public KafKaConnectorResource(Properties properties) {
		this.properties = properties;
	}

	@Override
	public ConsumerConnector connect() throws Exception {
		return connector = Consumer
				.createJavaConsumerConnector(createConsumerConfig());
	}

	private ConsumerConfig createConsumerConfig() throws Exception {
		Properties props = new Properties();
		String zkConnect = properties.get("zookeeper.connect").toString();
		this.zkClient = new ZkClient(zkConnect);
		String groupId = properties.get("group.id").toString();
		if (zkConnect == null || "".equals(zkConnect)) {
			throw new Exception("zookeeper.connect is null.");
		}
		if (groupId == null || "".equals(groupId)) {
			throw new Exception("group.id is null.");
		}
		props.put("zookeeper.connect", zkConnect);
		props.put("group.id", groupId);
		String sessionTimeOut = properties.get("zookeeper.session.timeout.ms")
				.toString();
		String syncTime = properties.get("zookeeper.sync.time.ms").toString();
		String autoCommit = properties.get("auto.commit.interval.ms")
				.toString();
		if (sessionTimeOut == null || "".equals(sessionTimeOut)) {
			sessionTimeOut = "30000";
		}
		if (syncTime == null || "".equals(syncTime)) {
			syncTime = "400";
		}
		if (autoCommit == null || "".equals(autoCommit)) {
			autoCommit = "2000";
		}
		props.put("zookeeper.session.timeout.ms", sessionTimeOut);
		props.put("zookeeper.sync.time.ms", syncTime);
		props.put("auto.commit.interval.ms", autoCommit);
		return new ConsumerConfig(props);
	}

	@Override
	public boolean check() {
		if (zkClient != null) {
			List<String> ids = zkClient.getChildren("/brokers/ids");
			boolean isNotEmpty = (ids != null && ids.size() > 0);
			if (!isNotEmpty) {
				log.warn(
						"lost kafka nodes in zookeeper folder /brokers/ids, reconnect again");
				this.close();
				return false;
			} else {
				return true;
			}
		}
		log.warn("no zk client instance available in weiwodb");
		return false;
	}

	@Override
	public void close() {
		if (connector != null) {
			connector.shutdown();
			connector = null;
		}
	}
}
