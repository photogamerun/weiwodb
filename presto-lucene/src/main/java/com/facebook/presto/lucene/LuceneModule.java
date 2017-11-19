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

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

import javax.inject.Inject;

import com.facebook.presto.lucene.hive.metastore.CachingLuceneMetastore;
import com.facebook.presto.lucene.hive.metastore.LuceneMetastore;
import com.facebook.presto.lucene.hive.metastore.LuceneMetastoreClientFactory;
import com.facebook.presto.lucene.hive.metastore.TableParameterCodec;
import com.facebook.presto.lucene.index.CacheIndexManager;
import com.facebook.presto.lucene.services.writer.SplitCompactService;
import com.facebook.presto.lucene.services.writer.WeiwoDBSplitWriterManager;
import com.facebook.presto.lucene.services.writer.WeiwoDataSourceService;
import com.facebook.presto.lucene.services.writer.ZookeeperLuceneFieldFetcher;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.RecoverAdapterInterface;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import io.airlift.log.Logger;
import io.airlift.units.Duration;

public class LuceneModule implements Module {

	private final String connectorId;
	private final TypeManager typeManager;
	private final NodeManager nodeManager;
	private final RecoverAdapterInterface recoverAdapter;
	private static final Logger log = Logger.get(LuceneModule.class);

	public LuceneModule(String connectorId, TypeManager typeManager,
			NodeManager nodeManager, RecoverAdapterInterface recoverAdapter) {
		this.connectorId = requireNonNull(connectorId, "connector id is null");
		this.typeManager = requireNonNull(typeManager, "typeManager is null");
		this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
		this.recoverAdapter = requireNonNull(recoverAdapter,
				"recoverAdapter is null");
	}

	@Override
	public void configure(Binder binder) {
		binder.bind(TypeManager.class).toInstance(typeManager);
		binder.bind(LuceneConnectorId.class)
				.toInstance(new LuceneConnectorId(connectorId));
		binder.bind(LuceneMetadata.class).in(Scopes.SINGLETON);
		binder.bind(TableParameterCodec.class).in(Scopes.SINGLETON);
		binder.bind(LuceneSplitManager.class).in(Scopes.SINGLETON);
		binder.bind(LuceneRecordSetProvider.class).in(Scopes.SINGLETON);
		binder.bind(CacheIndexManager.class).in(Scopes.SINGLETON);
		binder.bind(NodeManager.class).toInstance(nodeManager);
		binder.bind(RecoverAdapterInterface.class).toInstance(recoverAdapter);
		configBinder(binder).bindConfig(LuceneConfig.class);
		httpClientBinder(binder)
				.bindHttpClient("httpClient", ForWeiwoManager.class)
				.withConfigDefaults(config -> {
					config.setIdleTimeout(new Duration(30, SECONDS));
					config.setRequestTimeout(new Duration(10, SECONDS));
					config.setMaxConnectionsPerServer(250);
					config.setMaxRequestsQueuedPerDestination(10240);
				});

		binder.bind(LuceneMetastore.class).to(CachingLuceneMetastore.class)
				.in(Scopes.SINGLETON);

		log.info("bind hive metastore to CachingLuceneMetastore ");
		binder.bind(LuceneMetastoreClientFactory.class).in(Scopes.SINGLETON);

		binder.bind(TypeManager.class).toInstance(typeManager);

		binder.bind(LuceneMetadataFactory.class).in(Scopes.SINGLETON);
		binder.bind(WeiwoDBHdfsConfiguration.class).in(Scopes.SINGLETON);
		binder.bind(ZkClientFactory.class).in(Scopes.SINGLETON);
		binder.bind(LuceneHiveTableProperties.class).in(Scopes.SINGLETON);
		binder.bind(ZookeeperLuceneFieldFetcher.class).in(Scopes.SINGLETON);
		binder.bind(SplitCompactService.class).in(Scopes.SINGLETON);
		binder.bind(WeiwoDBSplitWriterManager.class).in(Scopes.SINGLETON);
		binder.bind(WeiwoDataSourceService.class).in(Scopes.SINGLETON);
	}

	public static final class TypeDeserializer
			extends
				FromStringDeserializer<Type> {
		private final TypeManager typeManager;

		@Inject
		public TypeDeserializer(TypeManager typeManager) {
			super(Type.class);
			this.typeManager = requireNonNull(typeManager,
					"typeManager is null");
		}

		@Override
		protected Type _deserialize(String value,
				DeserializationContext context) {
			Type type = typeManager.getType(parseTypeSignature(value));
			checkArgument(type != null, "Unknown type %s", value);
			return type;
		}
	}

}
