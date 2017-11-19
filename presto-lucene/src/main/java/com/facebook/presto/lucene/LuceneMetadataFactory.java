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

import javax.inject.Inject;

import com.facebook.presto.lucene.hive.metastore.LuceneMetastore;
import com.facebook.presto.spi.type.TypeManager;

public class LuceneMetadataFactory {

	private final LuceneMetastore client;
	private final LuceneConnectorId connectorId;
	private final TypeManager typeManager;
	private final ZkClientFactory zkClientFactory;
	private final LuceneConfig config;
	private final WeiwoDBHdfsConfiguration hdfsConfigration;

	@Inject
	public LuceneMetadataFactory(LuceneMetastore client,
			LuceneConnectorId connectorId, TypeManager typeManager,
			ZkClientFactory zkClientFactory, LuceneConfig config,
			WeiwoDBHdfsConfiguration hdfsConfigration) {
		this.client = requireNonNull(client, "client is null");
		this.connectorId = requireNonNull(connectorId, "connectorId is null");
		this.typeManager = requireNonNull(typeManager, "typeManager is null");
		this.zkClientFactory = requireNonNull(zkClientFactory,
				"zkClientFactory is null");
		this.config = requireNonNull(config, "config is null");
		this.hdfsConfigration = requireNonNull(hdfsConfigration,
				"hdfsConfigration is null");
	}

	public LuceneMetadata create() {
		return new LuceneMetadata(client, connectorId, typeManager,
				zkClientFactory, config, hdfsConfigration);
	}

}
