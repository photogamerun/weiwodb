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
package com.facebook.presto.lucene.services.writer;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import javax.inject.Inject;
import javax.ws.rs.core.Response.Status;

import com.alibaba.fastjson.JSON;
import com.facebook.presto.lucene.ForWeiwoManager;
import com.facebook.presto.lucene.LuceneConfig;
import com.facebook.presto.lucene.base.DataSourceAndNumber;
import com.facebook.presto.lucene.base.DataSourceNumber;
import com.facebook.presto.lucene.base.HeartBeatCommand;
import com.facebook.presto.lucene.base.HostDataSources;
import com.facebook.presto.lucene.services.writer.util.WeiwoDBClassUtils;
import com.facebook.presto.lucene.util.JsonCodecUtils;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;

import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

public class WeiwoDataSourceService {

	private static final Logger log = Logger.get(WeiwoDataSourceService.class);

	public final int defaultHeartInterval = 60 * 1000;

	final HostAddress hostAddress;
	final LuceneConfig config;
	final Map<String, List<WeiwoDBDataWriter>> currentSources;
	final List<WeiwoDBDataWriter> addedSources;
	final List<WeiwoDBDataWriter> deletedSources;
	final HttpClient httpClient;
	final NodeManager nodeManager;
	final JsonCodec<HostDataSources> hostDataSourcesCodec;
	final JsonCodec<HeartBeatCommand> heartBeatCommandCodec;
	final WeiwoDBSplitWriterManager splitWriterManager;
	boolean clear = false;
	final ClearService clearService;
	final Thread clearThread;

	@Inject
	public WeiwoDataSourceService(LuceneConfig config,
			@ForWeiwoManager HttpClient httpClient,
			WeiwoDBSplitWriterManager splitWriterManager,
			NodeManager nodeManager) {
		this.config = requireNonNull(config, "config is null");
		this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
		this.hostAddress = nodeManager.getCurrentNode().getHostAndPort();
		this.httpClient = httpClient;
		this.currentSources = new HashMap<>();
		this.addedSources = new ArrayList<>();
		this.deletedSources = new ArrayList<>();
		this.hostDataSourcesCodec = JsonCodecUtils.getHostDataSourcesCodec();
		this.heartBeatCommandCodec = JsonCodecUtils.getHeartBeatCommandCodec();
		this.splitWriterManager = requireNonNull(splitWriterManager,
				"splitWriterManager is null");
		this.clearService = new ClearService();
		this.clearThread = new Thread(clearService);
		clearThread.setDaemon(true);
		clearThread.setName("Init Clear Register Info Thread");
		clearThread.start();
		Timer timer = new Timer("HeartBeatService Thread", true);
		timer.schedule(new HeartBeatService(), defaultHeartInterval,
				defaultHeartInterval);
	}

	void processCommand(HeartBeatCommand command) {
		processAdds(command.getAdd());
		processDeletes(command.getDelete());
	}

	private void processAdds(List<DataSourceAndNumber> adds) {
		for (DataSourceAndNumber add : adds) {
			if (currentSources.get(add.getSource().getName()) != null
					&& currentSources.get(add.getSource().getName())
							.contains(new WeiwoDBDataWriter(config.getNodeId(),
									add.getSource().getName(),
									add.getNum().getNumber()) {
								@Override
								public void init() throws Exception {
								}

								@Override
								public void close() throws Exception {
								}
							})) {
				log.warn("Source exist. source = " + JSON.toJSONString(add));
			} else {
				log.info("Add data source. source = " + JSON.toJSONString(add));
				addDataSource(add);
			}
		}
	}

	private void processDeletes(List<DataSourceAndNumber> deteles) {
		for (DataSourceAndNumber delete : deteles) {
			if (currentSources.get(delete.getSource().getName()) != null
					&& currentSources.get(delete.getSource().getName())
							.contains(new WeiwoDBDataWriter(config.getNodeId(),
									delete.getSource().getName(), delete
											.getNum().getNumber()) {
								@Override
								public void init() throws Exception {
								}

								@Override
								public void close() throws Exception {
								}
							})) {
				log.info(
						"Delete source. source = " + JSON.toJSONString(delete));
				deleteDataSource(delete);
			} else {
				log.warn("Source not exist. source = "
						+ JSON.toJSONString(delete));
			}
		}
	}

	private void deleteDataSource(DataSourceAndNumber source) {
		List<WeiwoDBDataWriter> list = currentSources
				.get(source.getSource().getName());
		if (list != null) {
			int index = list.indexOf(new WeiwoDBDataWriter(config.getNodeId(),
					source.getSource().getName(), source.getNum().getNumber()) {
				@Override
				public void init() {
				}

				@Override
				public void close() {
				}
			});
			if (index >= 0) {
				WeiwoDBDataWriter writer = list.remove(index);
				try {
					writer.close();
				} catch (Exception e) {
					log.error(e, "Delete writer error. source = "
							+ JSON.toJSONString(source));
				}
				deletedSources.add(writer);
			}
		}
	}

	private void addDataSource(DataSourceAndNumber source) {
		Class<?> classz = WeiwoDBClassUtils
				.getClassByNameOrNull(source.getSource().getClassName());
		if (classz == null) {
			log.warn("Class not config or class not found. class name = "
					+ source.getSource().getClassName());
		} else {
			try {
				WeiwoDBDataWriter writer = (WeiwoDBDataWriter) WeiwoDBClassUtils
						.newInstance(classz);
				writer.setName(source.getSource().getName());

				// TODO current set id = nodeId.IF support concurrent for the
				// same source on node , the id must modify
				writer.setNodeId(config.getNodeId());
				writer.setNumber(source.getNum().getNumber());
				writer.setPathId(WriterIdUtil.genarate(writer.getNodeId(),
						writer.getNumber()));
				Properties pro = new Properties();
				pro.putAll(source.getSource().getProperties());
				writer.setProperties(pro);
				writer.setSplitWriterManager(splitWriterManager);
				writer.init();
				List<WeiwoDBDataWriter> list = currentSources
						.get(source.getSource().getName());
				if (list == null) {
					list = new ArrayList<>();
					currentSources.put(source.getSource().getName(), list);
				}
				list.add(writer);
				addedSources.add(writer);
			} catch (Exception e) {
				log.error(e, "Exception when create data writer. source = "
						+ JSON.toJSONString(source));
			}
		}
	}

	HeartBeatCommand sendHeartBeat(HostDataSources hostDataSources) {
		log.info("Send heartbeat to weiwomanager... info : "
				+ JSON.toJSONString(hostDataSources));
		HttpUriBuilder uriBuilder = uriBuilderFrom(
				URI.create(config.getWeiwomanagerUri()));
		uriBuilder.appendPath("/v1/datasource/heartbeat");
		Request request = preparePost().setUri(uriBuilder.build())
				.setHeader(HttpHeaders.CONTENT_TYPE,
						MediaType.JSON_UTF_8.toString())
				.setBodyGenerator(jsonBodyGenerator(hostDataSourcesCodec,
						hostDataSources))
				.build();
		JsonResponse<HeartBeatCommand> res = httpClient.execute(request,
				createFullJsonResponseHandler(heartBeatCommandCodec));
		if (res.getStatusCode() == Status.OK.getStatusCode()) {
			addedSources.clear();
			deletedSources.clear();
			return res.getValue();
		} else {
			log.warn(res.getException(),
					"Send Heartbeat error. status code = " + res.getStatusCode()
							+ ". status message = " + res.getStatusMessage());
			return null;
		}
	}

	boolean sendClear() {
		log.info("Send Clear to weiwomanager... info : " + config.getNodeId());
		HttpUriBuilder uriBuilder = uriBuilderFrom(
				URI.create(config.getWeiwomanagerUri()));
		uriBuilder.appendPath("/v1/index/clear");
		Request request = preparePost().setUri(uriBuilder.build())
				.setHeader(HttpHeaders.CONTENT_TYPE,
						MediaType.JSON_UTF_8.toString())
				.setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(
						config.getNodeId(), Charset.forName("UTF-8")))
				.build();
		StatusResponse res = httpClient.execute(request,
				createStatusResponseHandler());
		if (res.getStatusCode() == Status.OK.getStatusCode()) {
			log.info("Success clear for node = " + config.getNodeId());
			return true;
		} else {
			log.error("Failed clear for node = " + config.getNodeId() + ". "
					+ res.getStatusMessage());
			return false;
		}
	}

	HostDataSources createHostDataSources() {
		List<DataSourceNumber> sources = new ArrayList<>();
		List<DataSourceNumber> adds = new ArrayList<>();
		List<DataSourceNumber> deletes = new ArrayList<>();
		for (String name : currentSources.keySet()) {
			List<WeiwoDBDataWriter> list = currentSources.get(name);
			if (list != null) {
				for (WeiwoDBDataWriter writer : list) {
					sources.add(new DataSourceNumber(writer.getName(),
							writer.getNumber()));
				}
			}
		}
		for (WeiwoDBDataWriter writer : addedSources) {
			adds.add(
					new DataSourceNumber(writer.getName(), writer.getNumber()));
		}
		for (WeiwoDBDataWriter writer : deletedSources) {
			deletes.add(
					new DataSourceNumber(writer.getName(), writer.getNumber()));
		}
		HostDataSources hds = new HostDataSources(hostAddress, sources, adds,
				deletes);
		return hds;
	}

	class HeartBeatService extends TimerTask {

		@Override
		public void run() {
			try {
				if (clear) {
					HeartBeatCommand command = sendHeartBeat(
							createHostDataSources());
					if (command != null) {
						processCommand(command);
					}
				}
			} catch (Exception e) {
				log.error(e, "Exception in heartbeat service.");
			}
		}

	}

	class ClearService implements Runnable {

		@Override
		public void run() {
			log.info("Start init clear thread ...");
			while (!clear) {
				try {
					boolean success = sendClear();
					clear = success;
					Thread.sleep(3l * 1000l);
				} catch (Throwable t) {
					log.error(t, "Clear request error.");
				}
			}
			log.info("End init clear thread ...");
		}

	}

}
