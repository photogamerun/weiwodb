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

import java.util.List;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import io.airlift.configuration.Config;
/**
 * 
 * 
 * @author peter.wei
 *
 */
public class LuceneConfig {

	private static final Splitter SPLITTER = Splitter.on(',').trimResults()
			.omitEmptyStrings();

	private String dataPath;
	private String hiveMetaAddress;
	private int hiveMetaPort;
	private int timeOutMs;
	private int metaCacheTtlSec;
	private int metaRefreshIntervalSec;
	private int metaRefreshMaxThreads;
	private String weiwomanagerUri;
	private List<String> resourceConfigFiles;
	private int nodePort;
	private String nodeId;
	private String zkServers;
	private int sessionTimeout;
	private int connectionTimeout;
	private String zkPath;
	private int realIndexMaxSize;
	private int realIndexMaxTime;
	private int readerOpenDelay;
	private int multiReaderMaxNum;

	public int getReaderOpenDelay() {
		return readerOpenDelay;
	}

	@Config("lucene.realtime.reader.open.delay.sec")
	public void setReaderOpenDelay(int readerOpenDelay) {
		this.readerOpenDelay = readerOpenDelay;
	}

	@NotNull
	public int getRealIndexMaxSize() {
		return realIndexMaxSize;
	}

	@Config("lucene.realtime.index.max.size.mb")
	public void setRealIndexMaxSize(int realIndexMaxSize) {
		this.realIndexMaxSize = realIndexMaxSize;
	}

	@NotNull
	public int getRealIndexMaxTime() {
		return realIndexMaxTime;
	}

	@Config("lucene.realtime.index.max.time.sec")
	public void setRealIndexMaxTime(int realIndexMaxTime) {
		this.realIndexMaxTime = realIndexMaxTime;
	}

	@NotNull
	public String getZkPath() {
		return zkPath;
	}

	@Config("zk.path")
	public void setZkPath(String zkPath) {
		this.zkPath = zkPath;
	}

	@NotNull
	public String getZkServers() {
		return zkServers;
	}

	@Config("zk.servers")
	public void setZkServers(String zkServers) {
		this.zkServers = zkServers;
	}

	@NotNull
	public int getSessionTimeout() {
		return sessionTimeout;
	}

	@Config("zk.session.timeout.ms")
	public void setSessionTimeout(int sessionTimeout) {
		this.sessionTimeout = sessionTimeout;
	}

	@NotNull
	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	@Config("zk.connection.timeout.ms")
	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	@NotNull
	public String getNodeId() {
		return nodeId;
	}

	@Config("node.id")
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	@NotNull
	public int getNodePort() {
		return nodePort;
	}

	@Config("lucene.node.port")
	public void setNodePort(int nodePort) {
		this.nodePort = nodePort;
	}

	@NotNull
	public String getWeiwomanagerUri() {
		return weiwomanagerUri;
	}

	@Config("lucene.weiwo.manager.uri")
	public void setWeiwomanagerUri(String weiwomanagerUri) {
		this.weiwomanagerUri = weiwomanagerUri;
	}

	@NotNull
	public String getDataPath() {
		return dataPath;
	}

	@Config("lucene.data.path")
	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	@NotNull
	public String getHiveMetaAddress() {
		return hiveMetaAddress;
	}

	@Config("lucene.hive.meta.address")
	public void setHiveMetaAddress(String hiveMetaAddress) {
		this.hiveMetaAddress = hiveMetaAddress;
	}

	@NotNull
	public int getHiveMetaPort() {
		return hiveMetaPort;
	}

	@Config("lucene.hive.meta.port")
	public void setHiveMetaPort(int hiveMetaPort) {
		this.hiveMetaPort = hiveMetaPort;
	}

	@NotNull
	public int getTimeOutMs() {
		return timeOutMs;
	}

	@Config("lucene.hive.meta.time.out.ms")
	public void setTimeOutMs(int timeOutMs) {
		this.timeOutMs = timeOutMs;
	}

	@NotNull
	public int getMetaCacheTtlSec() {
		return metaCacheTtlSec;
	}

	@Config("lucene.hive.meta.cache.ttl.sec")
	public void setMetaCacheTtlSec(int metaCacheTtlSec) {
		this.metaCacheTtlSec = metaCacheTtlSec;
	}

	@NotNull
	public int getMetaRefreshIntervalSec() {
		return metaRefreshIntervalSec;
	}

	@Config("lucene.hive.meta.cache.refresh.interval.sec")
	public void setMetaRefreshIntervalSec(int metaRefreshIntervalSec) {
		this.metaRefreshIntervalSec = metaRefreshIntervalSec;
	}

	@NotNull
	@Min(1)
	public int getMetaRefreshMaxThreads() {
		return metaRefreshMaxThreads;
	}

	@Config("lucene.hive.meta.refresh.max.threads")
	public void setMetaRefreshMaxThreads(int metaRefreshMaxThreads) {
		this.metaRefreshMaxThreads = metaRefreshMaxThreads;
	}

	public List<String> getResourceConfigFiles() {
		return resourceConfigFiles;
	}

	@Config("lucene.config.resources")
	public LuceneConfig setResourceConfigFiles(String files) {
		this.resourceConfigFiles = (files == null)
				? null
				: SPLITTER.splitToList(files);
		return this;
	}

	@NotNull
	@Min(1)
	public int getMultiReaderMaxNum() {
		return multiReaderMaxNum;
	}

	@Config("lucene.multi.reader.max.num")
	public void setMultiReaderMaxNum(int multiReaderMaxNum) {
		this.multiReaderMaxNum = multiReaderMaxNum;
	}

	public LuceneConfig setResourceConfigFiles(List<String> files) {
		this.resourceConfigFiles = (files == null)
				? null
				: ImmutableList.copyOf(files);
		return this;
	}
}