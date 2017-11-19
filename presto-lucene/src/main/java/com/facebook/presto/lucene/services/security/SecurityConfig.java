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
package com.facebook.presto.lucene.services.security;

import io.airlift.configuration.Config;
/**
 * 
 * @author peter.wei
 *
 */
public class SecurityConfig {

	public static final String ALLOW_ALL_ACCESS_CONTROL = "allow-all";

	public static final String WEIWO_AUTHORIZATION = "weiwo_authorization";

	private String authentication = ALLOW_ALL_ACCESS_CONTROL;

	private String authorization = ALLOW_ALL_ACCESS_CONTROL;

	private boolean allowDropTable;

	private boolean allowRenameTable;

	public String getAuthentication() {
		return authentication;
	}

	public String getAuthorization() {
		return authorization;
	}

	@Config("authentication")
	public SecurityConfig setAuthentication(String authentication) {
		this.authentication = authentication;
		return this;
	}

	@Config("authorization")
	public SecurityConfig setAuthorization(String authorization) {
		this.authorization = authorization;
		return this;
	}

	@Config("allow-drop-table")
	public void setAllowDropTable(boolean allowDropTable) {
		this.allowDropTable = allowDropTable;
	}

	@Config("allow-rename-table")
	public void setAllowRenameTable(boolean allowRenameTable) {
		this.allowRenameTable = allowRenameTable;
	}

	public boolean isAllowRenameTable() {
		return allowRenameTable;
	}

	public boolean isAllowDropTable() {
		return allowDropTable;
	}
}