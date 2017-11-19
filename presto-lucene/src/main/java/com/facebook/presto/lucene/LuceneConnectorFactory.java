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

import java.util.Map;

import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;
import com.facebook.presto.lucene.services.security.SecurityConfig;
import com.facebook.presto.lucene.services.security.WeiwoAuthenticationModules;
import com.facebook.presto.lucene.services.security.WeiwoAuthorizationModules;
import com.facebook.presto.lucene.wltea.analyzer.cfg.Configuration;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.RecoverAdapterInterface;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;

import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
/**
 * 
 * @author peter.wei
 *
 */
public class LuceneConnectorFactory implements ConnectorFactory {

	private final TypeManager typeManager;
	private final Map<String, String> optionalConfig;
	private final NodeManager nodeManager;
	private final RecoverAdapterInterface recoverAdapter;
	private static final Logger log = Logger.get(LuceneConnectorFactory.class);

	public LuceneConnectorFactory(TypeManager typeManager,
			Map<String, String> optionalConfig, NodeManager nodeManager,
			RecoverAdapterInterface recoverAdapter) {
		this.typeManager = requireNonNull(typeManager, "typeManager is null");
		this.optionalConfig = ImmutableMap.copyOf(
				requireNonNull(optionalConfig, "optionalConfig is null"));
		this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
		this.recoverAdapter = requireNonNull(recoverAdapter,
				"recoverAdapter is null");
	}

	@Override
	public String getName() {
		return "weiwodb";
	}

	@Override
	public ConnectorHandleResolver getHandleResolver() {
		return new LuceneHandleResolver();
	}

	@Override
	public Connector create(String connectorId,
			Map<String, String> requiredConfig) {
		requireNonNull(requiredConfig, "requiredConfig is null");
		requireNonNull(optionalConfig, "optionalConfig is null");

		try {
			Bootstrap app = new Bootstrap(
					new LuceneModule(connectorId, typeManager, nodeManager,
							recoverAdapter),
					ConditionalModule.conditionalModule(SecurityConfig.class,
							security -> SecurityConfig.ALLOW_ALL_ACCESS_CONTROL
									.equalsIgnoreCase(
											security.getAuthorization()),
							WeiwoAuthorizationModules
									.noMetastoreAuthorizationModule()),
					ConditionalModule.conditionalModule(SecurityConfig.class,
							security -> SecurityConfig.WEIWO_AUTHORIZATION
									.equalsIgnoreCase(
											security.getAuthorization()),
							WeiwoAuthorizationModules
									.weiwoMetastoreAuthorizationModule()),
					ConditionalModule.conditionalModule(SecurityConfig.class,
							security -> SecurityConfig.ALLOW_ALL_ACCESS_CONTROL
									.equalsIgnoreCase(
											security.getAuthentication()),
							WeiwoAuthenticationModules
									.noMetastoreAuthenticationModule()));

			Injector injector = app.strictConfig().doNotInitializeLogging()
					.setRequiredConfigurationProperties(requiredConfig)
					.setOptionalConfigurationProperties(optionalConfig)
					.initialize();

			Configuration.conf_dir = optionalConfig
					.get(WeiwoDBConfigureKeys.PROJECT_PATH);

			LifeCycleManager lifeCycleManager = injector
					.getInstance(LifeCycleManager.class);
			LuceneSplitManager splitManager = injector
					.getInstance(LuceneSplitManager.class);
			LuceneHiveTableProperties lucenehiveTableProperties = injector
					.getInstance(LuceneHiveTableProperties.class);
			LuceneMetadata metadata = injector
					.getInstance(LuceneMetadata.class);
			LuceneRecordSetProvider recordSetProvider = injector
					.getInstance(LuceneRecordSetProvider.class);

			ConnectorAccessControl accessControl = injector
					.getInstance(ConnectorAccessControl.class);

			log.info("start with AccessControl " + accessControl);

			return new LuceneConnector(lifeCycleManager, metadata, splitManager,
					recordSetProvider,
					lucenehiveTableProperties.getTableProperties(),
					accessControl);
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}
}