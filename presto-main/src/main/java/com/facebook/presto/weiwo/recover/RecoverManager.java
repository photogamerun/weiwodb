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
package com.facebook.presto.weiwo.recover;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.util.Objects.requireNonNull;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.core.Response.Status;

import com.alibaba.fastjson.JSON;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;
import com.facebook.presto.metadata.ForNodeManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.spi.Node;
import com.facebook.presto.weiwo.manager.WeiwoZkClientFactory;
import com.google.common.base.Joiner;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.log.Logger;

public class RecoverManager {

	private static final Logger log = Logger.get(RecoverManager.class);

	final WeiwoZkClientFactory zkClientFactory;

	final Map<String, Map<String, WriterInfo>> recoveringWriters;
	final Map<String, Map<String, WriterInfo>> needRecoverWriters;
	final NodeSchedulerConfig nodeSchedulerConfig;
	ServerConfig config;
	final String writerPath;
	InternalNodeManager nodeManager;
	HttpClient httpClient;
	final ExecutorService startService;

	// after start weiwomanager , during this time , recover will just schedule
	// to node with the same nodeId.
	// after this time , the writer may be scheduled to other node
	public long startTime = 0;
	public long recoverAbateTime = 3 * 60 * 1000;
	public boolean recoverAbate = true;

	// is weiwomanager in start recover progress , if true data source will not
	// schedule
	public boolean startRecover = true;

	final static long interval = 10 * 1000;

	@Inject
	public RecoverManager(WeiwoZkClientFactory zkClientFactory,
			ServerConfig config, @ForNodeManager HttpClient httpClient,
			InternalNodeManager nodeManager,
			NodeSchedulerConfig nodeSchedulerConfig) {
		this.zkClientFactory = requireNonNull(zkClientFactory,
				"zkClientFactory is null");
		this.config = requireNonNull(config, "config is null");
		this.nodeSchedulerConfig = requireNonNull(nodeSchedulerConfig,
				"nodeSchedulerConfig is null");
		this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
		this.httpClient = requireNonNull(httpClient, "httpClient is null");
		this.recoveringWriters = new ConcurrentHashMap<>();
		this.needRecoverWriters = new ConcurrentHashMap<>();
		this.writerPath = Joiner.on(WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR)
				.join(config.getZkPath(),
						WeiwoDBConfigureKeys.WEIWO_WRITER_PATH);
		this.startTime = System.currentTimeMillis();
		this.startService = Executors.newFixedThreadPool(4);

		Timer timer = new Timer("RecoverCheck Thread", true);
		timer.schedule(new RecoverCheck(), interval, interval);
	}

	public void recoverCheck() {
		if (recoverAbate && (System.currentTimeMillis()
				- startTime) > recoverAbateTime) {
			recoverAbate = false;
		}
		if (zkClientFactory.getZkClient().exists(writerPath)) {
			log.info("RecoverCheck...");
			List<String> nodes = zkClientFactory.getZkClient()
					.getChildren(writerPath);
			needRecoverWriters.clear();
			if (nodes != null) {
				nodes.forEach(node -> processPerNode(node));
			}
		}
		if (!recoverAbate && startRecover) {
			if (needRecoverWriters.size() == 0
					&& recoveringWriters.size() == 0) {
				startRecover = false;
			}
		}
	}

	public void completeRecover(String nodeId, String indexName) {
		log.info("Complete recover writer nodeId = : " + nodeId
				+ ". indexName = " + indexName);
		synchronized (recoveringWriters) {
			if (recoveringWriters.get(nodeId) != null) {
				recoveringWriters.get(nodeId).remove(indexName);
				if (recoveringWriters.get(nodeId).size() == 0) {
					recoveringWriters.remove(nodeId);
				}
			}
		}
	}

	public boolean isNodeNeedRecover(String nodeId) {
		if (needRecoverWriters.get(nodeId) != null
				&& needRecoverWriters.get(nodeId).keySet().size() > 0) {
			return true;
		} else {
			List<String> off = getOfflineWriters(nodeId);
			if (off != null && off.size() > 0) {
				return true;
			}
			return false;
		}
	}

	public boolean isNodeRecovering(String nodeId) {
		if (recoveringWriters.get(nodeId) != null
				&& recoveringWriters.get(nodeId).keySet().size() > 0) {
			return true;
		} else {
			return false;
		}
	}

	private List<String> getOfflineWriters(String nodeId) {
		List<String> writings = zkClientFactory.getZkClient()
				.getChildren(writerPath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
						+ nodeId + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
						+ WeiwoDBConfigureKeys.WEIWO_WRITER_WRITING_PATH);
		List<String> alives = zkClientFactory.getZkClient()
				.getChildren(writerPath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
						+ nodeId + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
						+ WeiwoDBConfigureKeys.WEIWO_WRITER_ALIVE_PATH);
		if (writings != null) {
			if (alives != null) {
				writings.removeAll(alives);
			}
			return writings;
		} else {
			return new ArrayList<>();
		}
	}

	private void processPerNode(String nodeId) {
		if (zkClientFactory.getZkClient()
				.exists(writerPath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
						+ nodeId + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
						+ WeiwoDBConfigureKeys.WEIWO_WRITER_WRITING_PATH)
				&& zkClientFactory.getZkClient().exists(writerPath
						+ WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + nodeId
						+ WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
						+ WeiwoDBConfigureKeys.WEIWO_WRITER_ALIVE_PATH)) {
			log.info("Process node = " + nodeId);
			List<String> writings = getOfflineWriters(nodeId);
			if (writings.size() > 0) {
				Node node = getActiveNode(nodeId);
				if (node != null) {
					for (String indexName : writings) {
						if (recoveringWriters.get(nodeId) != null
								&& recoveringWriters.get(nodeId)
										.get(indexName) != null) {
							log.info("Index name already in recovering... : "
									+ indexName);
						} else {
							String data = zkClientFactory.getZkClient()
									.readData(writerPath
											+ WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
											+ nodeId
											+ WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
											+ WeiwoDBConfigureKeys.WEIWO_WRITER_WRITING_PATH
											+ WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
											+ indexName);
							Map<String, Object> properties = JSON
									.parseObject(data, Map.class);
							Map<String, WriterInfo> nodeMap = needRecoverWriters
									.get(nodeId);
							if (nodeMap == null) {
								nodeMap = new ConcurrentHashMap<>();
								needRecoverWriters.put(nodeId, nodeMap);
							}
							nodeMap.put(indexName, new WriterInfo(indexName,
									properties, 0, null));
							startRecover(node, indexName, properties);
						}
					}
				} else {
					if (!recoverAbate) {
						for (String indexName : writings) {
							if (recoveringWriters.get(nodeId) != null
									&& recoveringWriters.get(nodeId)
											.get(indexName) != null) {
								log.warn(
										"Index name already in recovering... node = "
												+ nodeId
												+ ". And need remove recover info : "
												+ indexName);
								synchronized (recoveringWriters) {
									recoveringWriters.get(nodeId)
											.remove(indexName);
									if (recoveringWriters.get(nodeId)
											.size() == 0) {
										recoveringWriters.remove(nodeId);
									}
								}
							} else {
								String data = zkClientFactory.getZkClient()
										.readData(writerPath
												+ WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
												+ nodeId
												+ WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
												+ WeiwoDBConfigureKeys.WEIWO_WRITER_WRITING_PATH
												+ WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
												+ indexName);
								Map<String, Object> properties = JSON
										.parseObject(data, Map.class);
								Map<String, WriterInfo> nodeMap = needRecoverWriters
										.get(nodeId);
								if (nodeMap == null) {
									nodeMap = new ConcurrentHashMap<>();
									needRecoverWriters.put(nodeId, nodeMap);
								}
								nodeMap.put(indexName, new WriterInfo(indexName,
										properties, 0, null));
								Node selectNode = getRandomActiveNode();
								if (selectNode != null) {
									startRecover(selectNode, indexName,
											properties);
								} else {
									log.error(
											"Can not find available node to recover : "
													+ data);
								}
							}
						}
					}
				}
			}
		}
	}

	private Node getRandomActiveNode() {
		List<Node> anodes = nodeManager.getAllNodes().getActiveNodes().stream()
				.filter(node -> nodeSchedulerConfig.isIncludeCoordinator()
						|| !nodeManager.getCoordinators().contains(node))
				.filter(node -> nodeSchedulerConfig.isIncludeWeiwoManager()
						|| !nodeManager.getWeiwomanagers().contains(node))
				.collect(Collectors.toList());
		if (anodes.size() > 0) {
			Random random = new Random();
			int index = random.nextInt(anodes.size());
			return anodes.get(index);
		} else {
			log.warn("There is no node in active state.");
			return null;
		}
	}

	private Node getActiveNode(String nodeId) {
		if (nodeId != null && !"".equals(nodeId)) {
			Set<Node> anodes = nodeManager.getAllNodes().getActiveNodes();
			List<Node> remoteaNode = new ArrayList<>();
			anodes.forEach(node -> {
				if (nodeId.equals(node.getNodeIdentifier())) {
					if ((nodeSchedulerConfig.isIncludeCoordinator()
							|| !nodeManager.getCoordinators().contains(node))
							&& (nodeSchedulerConfig.isIncludeWeiwoManager()
									|| !nodeManager.getWeiwomanagers()
											.contains(node))) {
						remoteaNode.add(node);
					}
				}
			});
			if (remoteaNode.size() == 1) {
				return remoteaNode.get(0);
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	private Node getShutdownNode(String nodeId) {
		if (nodeId != null && !"".equals(nodeId)) {
			Set<Node> snodes = nodeManager.getAllNodes().getShuttingDownNodes();
			List<Node> remotesNode = new ArrayList<>();
			snodes.forEach(node -> {
				if (nodeId.equals(node.getNodeIdentifier())) {
					remotesNode.add(node);
				}
			});
			if (remotesNode.size() == 1) {
				return remotesNode.get(0);
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	private void startRecover(Node node, String indexName,
			Map<String, Object> properties) {
		startService.submit(new StartRemoteTask(node, indexName, properties));
	}

	private void startRemoteRecover(Node node, String indexName,
			Map<String, Object> properties) {
		log.info("Send start request for [" + indexName + "] to "
				+ node.toString());
		HttpUriBuilder uriBuilder = uriBuilderFrom(node.getHttpUri());
		uriBuilder.appendPath("/v1/recover/start");
		Request request = preparePost().setUri(uriBuilder.build())
				.setHeader(HttpHeaders.CONTENT_TYPE,
						MediaType.JSON_UTF_8.toString())
				.setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(
						JSON.toJSONString(properties),
						Charset.forName("UTF-8")))
				.build();
		StatusResponse res = httpClient.execute(request,
				createStatusResponseHandler());
		if (res.getStatusCode() == Status.OK.getStatusCode()) {
			log.info("Success start Recover for [" + indexName + "] to "
					+ node.toString());
			synchronized (recoveringWriters) {
				Map<String, WriterInfo> nodeMap = recoveringWriters
						.get(node.getNodeIdentifier());
				if (nodeMap == null) {
					nodeMap = new ConcurrentHashMap<>();
					recoveringWriters.put(node.getNodeIdentifier(), nodeMap);
				}
				nodeMap.put(indexName, new WriterInfo(indexName, properties,
						System.currentTimeMillis(), node));
			}
		} else {
			log.error("Failed start Recover for [" + indexName + "] to "
					+ node.toString());
		}
	}

	class StartRemoteTask implements Runnable {
		Node node;
		String indexName;
		Map<String, Object> properties;

		public StartRemoteTask(Node node, String indexName,
				Map<String, Object> properties) {
			this.node = requireNonNull(node, "node is null");
			this.indexName = requireNonNull(indexName, "indexName is null");
			this.properties = requireNonNull(properties, "properties is null");
		}

		@Override
		public void run() {
			startRemoteRecover(node, indexName, properties);
		}

	}

	class RecoverCheck extends TimerTask {

		@Override
		public void run() {
			if (config.isReadOnly()) {
				log.info("read only model won't check recover");
			} else {
				recoverCheck();
			}
		}
	}
}
