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
package com.facebook.presto.split;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.pushdown.PushDown;
import com.facebook.presto.sql.pair.GroupByPair;
import com.facebook.presto.sql.pair.OutputSignature;
import com.facebook.presto.sql.pair.SimpleFilterPair;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.WeiwoPushDownNode;
import com.facebook.presto.util.PushDownUtilities;

public class SplitManager {
	private final ConcurrentMap<String, ConnectorSplitManager> splitManagers = new ConcurrentHashMap<>();

	public void addConnectorSplitManager(String connectorId,
			ConnectorSplitManager connectorSplitManager) {
		checkState(
				splitManagers.putIfAbsent(connectorId,
						connectorSplitManager) == null,
				"SplitManager for connector '%s' is already registered",
				connectorId);
	}

	public SplitSource getSplits(Session session, TableLayoutHandle layout,
			WeiwoPushDownNode node) {

		// handle filter push down, handle aggregation push down
		PushDown pushdown = new PushDown();
		if (node != null) {
			// if(PushDownUtilities.isPushDownFunction(node.getFunctions().values())){
			// pushdown.setFunction(PushDownUtilities
			// .extractAggregationFunction(node.getFunctions().values()));
			// }
			if (PushDownUtilities
					.isPushDownFilter(node.getOriginalConstraint())) {
				pushdown.addPair(
						new SimpleFilterPair(node.getOriginalConstraint()));
				List<Symbol> symbol = node.getGroupbykeys();
				if (symbol != null && symbol.size() > 0) {
					pushdown.addPair(
							new GroupByPair(
									symbol.stream()
											.map(item -> new OutputSignature(
													item, -1))
											.collect(Collectors.toList())));
				}
			}
		}

		String connectorId = layout.getConnectorId();
		ConnectorSplitManager splitManager = getConnectorSplitManager(
				connectorId);

		// assumes connectorId and catalog are the same
		ConnectorSession connectorSession = session
				.toConnectorSession(connectorId);

		ConnectorSplitSource source = splitManager.getSplits(
				layout.getTransactionHandle(), connectorSession,
				layout.getConnectorHandle(), pushdown);

		return new ConnectorAwareSplitSource(connectorId,
				layout.getTransactionHandle(), source);
	}

	private ConnectorSplitManager getConnectorSplitManager(String connectorId) {
		ConnectorSplitManager result = splitManagers.get(connectorId);
		checkArgument(result != null, "No split manager for connector '%s'",
				connectorId);
		return result;
	}
}
