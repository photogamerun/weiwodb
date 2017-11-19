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
package com.facebook.presto.server;

import static java.util.concurrent.TimeUnit.MINUTES;

import javax.validation.constraints.NotNull;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

public class ServerConfig {
	private boolean coordinator = true;
	private boolean weiwomanager = false;
	private String prestoVersion;
	private String dataSources;
	private boolean includeExceptionInResponse = true;
	private Duration gracePeriod = new Duration(2, MINUTES);
	private String zkServers;
	private int sessionTimeout;
	private int connectionTimeout;
	private String zkPath;
	private String managerUri;
	private boolean readOnly;

	public boolean isReadOnly() {
		return readOnly;
	}

	@Config("readonly")
	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}

	public boolean isCoordinator() {
		return coordinator;
	}

	@Config("coordinator")
	public ServerConfig setCoordinator(boolean coordinator) {
		this.coordinator = coordinator;
		return this;
	}

	public boolean isWeiwomanager() {
		return weiwomanager;
	}

	@Config("weiwomanager")
	public ServerConfig setWeiwomanager(boolean weiwomanager) {
		this.weiwomanager = weiwomanager;
		return this;
	}

	public String getPrestoVersion() {
		return prestoVersion;
	}

	@Config("presto.version")
	public ServerConfig setPrestoVersion(String prestoVersion) {
		this.prestoVersion = prestoVersion;
		return this;
	}

	@Deprecated
	public String getDataSources() {
		return dataSources;
	}

	@Deprecated
	@Config("datasources")
	public ServerConfig setDataSources(String dataSources) {
		this.dataSources = dataSources;
		return this;
	}

	public boolean isIncludeExceptionInResponse() {
		return includeExceptionInResponse;
	}

	@Config("http.include-exception-in-response")
	public ServerConfig setIncludeExceptionInResponse(
			boolean includeExceptionInResponse) {
		this.includeExceptionInResponse = includeExceptionInResponse;
		return this;
	}

	public Duration getGracePeriod() {
		return gracePeriod;
	}

	@Config("shutdown.grace-period")
	public ServerConfig setGracePeriod(Duration gracePeriod) {
		this.gracePeriod = gracePeriod;
		return this;
	}

	@NotNull
	public String getManagerUri() {
		return managerUri;
	}

	@Config("manager.uri")
	public void setManagerUri(String managerUri) {
		this.managerUri = managerUri;
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
}
