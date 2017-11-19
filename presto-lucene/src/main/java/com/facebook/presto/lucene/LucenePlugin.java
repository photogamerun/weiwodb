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

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.RecoverAdapterInterface;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class LucenePlugin implements Plugin {

	private TypeManager typeManager;
	private NodeManager nodeManager;
	RecoverAdapterInterface recoverAdapter;
	private Map<String, String> optionalConfig = ImmutableMap.of();

	@Override
	public void setOptionalConfig(Map<String, String> optionalConfig) {
		this.optionalConfig = ImmutableMap.copyOf(
				requireNonNull(optionalConfig, "optionalConfig is null"));
	}

	@Inject
	public void setTypeManager(TypeManager typeManager) {
		this.typeManager = typeManager;
	}

	@Inject
	public void setNodeManager(NodeManager nodeManager) {
		this.nodeManager = nodeManager;
	}

	@Inject
	public void setRecoverAdapter(RecoverAdapterInterface recoverAdapter) {
		this.recoverAdapter = recoverAdapter;
	}

	public Map<String, String> getOptionalConfig() {
		return optionalConfig;
	}

	@Override
	public <T> List<T> getServices(Class<T> type) {
		if (type == ConnectorFactory.class) {
			return ImmutableList
					.of(type.cast(new LuceneConnectorFactory(typeManager,
							getOptionalConfig(), nodeManager, recoverAdapter)));
		}
		return ImmutableList.of();
	}

}
