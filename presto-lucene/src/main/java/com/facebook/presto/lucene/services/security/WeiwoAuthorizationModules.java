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

import static com.google.inject.Scopes.SINGLETON;

import com.facebook.presto.lucene.services.security.internals.NoAccessControl;
import com.facebook.presto.lucene.services.security.internals.WeiwoSqlAccessControl;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.google.inject.Module;
import com.google.inject.Scopes;
/**
 * 
 * @author peter.wei
 *
 */
public class WeiwoAuthorizationModules {

	public static Module weiwoMetastoreAuthorizationModule() {
		return binder -> binder.bind(ConnectorAccessControl.class)
				.to(WeiwoSqlAccessControl.class).in(SINGLETON);
	}

	public static Module noMetastoreAuthorizationModule() {
		return binder -> binder.bind(ConnectorAccessControl.class)
				.to(NoAccessControl.class).in(Scopes.SINGLETON);
	}
}