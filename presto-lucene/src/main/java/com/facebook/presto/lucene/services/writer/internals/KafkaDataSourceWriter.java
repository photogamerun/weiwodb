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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.facebook.presto.lucene.services.writer.WeiwoDBDataWriter;

import io.airlift.log.Logger;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaDataSourceWriter extends WeiwoDBDataWriter {

	private static final Logger log = Logger.get(KafkaDataSourceWriter.class);
	private ConsumerConnector consumer;
	private ExecutorService executor;
	String topic;
	private int threadNum;

	@Override
	public void init() throws Exception {
		this.threadNum = Integer.valueOf(
				this.getProperties().get("consumer.thread.num").toString());
		this.topic = this.getProperties().get("kafka.topic.name").toString();
		this.consumer = Consumer
				.createJavaConsumerConnector(createConsumerConfig());
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, threadNum);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> list = consumerMap.get(this.topic);
		this.executor = Executors.newFixedThreadPool(threadNum);
		int i = 0;
		for (KafkaStream<byte[], byte[]> stream : list) {
			this.executor
					.submit(new KafkaDataSourceWriterThread(stream, this, i));
			i++;
		}
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				log.info("invoke shutdown hook to shutdown kafka data source");
				close();
			} catch (Exception e) {
				log.error(e, "fail to shutdown kafka connector resource");
			}
		}));
	}

	@Override
	public void close() throws Exception {
		if (consumer != null) {
			consumer.shutdown();
			consumer = null;
		}
		if (executor != null) {
			executor.shutdown();
			executor = null;
		}
	}

	private ConsumerConfig createConsumerConfig() throws Exception {
		Properties props = new Properties();
		String zkConnect = this.getProperties().get("zookeeper.connect")
				.toString();
		String groupId = this.getProperties().get("group.id").toString();
		if (zkConnect == null || "".equals(zkConnect)) {
			throw new Exception("zookeeper.connect is null.");
		}
		if (groupId == null || "".equals(groupId)) {
			throw new Exception("group.id is null.");
		}
		props.put("zookeeper.connect", zkConnect);
		props.put("group.id", groupId);
		String sessionTimeOut = this.getProperties()
				.get("zookeeper.session.timeout.ms").toString();
		String syncTime = this.getProperties().get("zookeeper.sync.time.ms")
				.toString();
		String autoCommit = this.getProperties().get("auto.commit.interval.ms")
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
}