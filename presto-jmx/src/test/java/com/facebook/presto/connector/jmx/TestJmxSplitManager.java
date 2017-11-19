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
package com.facebook.presto.connector.jmx;

import static com.facebook.presto.connector.jmx.JmxMetadata.HISTORY_SCHEMA_NAME;
import static com.facebook.presto.connector.jmx.JmxMetadata.JMX_SCHEMA_NAME;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.pushdown.PushDown;
import com.facebook.presto.spi.type.TimestampType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.airlift.units.Duration;

public class TestJmxSplitManager {
	private static final Duration JMX_STATS_DUMP = new Duration(100,
			TimeUnit.MILLISECONDS);
	private static final long SLEEP_TIME = JMX_STATS_DUMP.toMillis() / 5;
	private static final long TIMEOUT_TIME = JMX_STATS_DUMP.toMillis() * 40;
	private static final String TEST_BEANS = "java.lang:type=Runtime";
	private static final String CONNECTOR_ID = "test-id";
	private final Node localNode = new TestingNode("host1");
	private final Set<Node> nodes = ImmutableSet.of(localNode,
			new TestingNode("host2"), new TestingNode("host3"));

	private final JmxConnector jmxConnector = (JmxConnector) new JmxConnectorFactory(
			getPlatformMBeanServer(), new TestingNodeManager()).create(
					CONNECTOR_ID,
					ImmutableMap.of("jmx.dump-tables", TEST_BEANS,
							"jmx.dump-period",
							format("%dms", JMX_STATS_DUMP.toMillis()),
							"jmx.max-entries", "1000"));

	private final JmxColumnHandle columnHandle = new JmxColumnHandle("test",
			"node", createUnboundedVarcharType());
	private final JmxTableHandle tableHandle = new JmxTableHandle("test",
			"objectName", ImmutableList.of(columnHandle), true);

	private final JmxSplitManager splitManager = jmxConnector.getSplitManager();
	private final JmxMetadata metadata = jmxConnector
			.getMetadata(new ConnectorTransactionHandle() {
			});
	private final JmxRecordSetProvider recordSetProvider = jmxConnector
			.getRecordSetProvider();

	@AfterClass
	public void tearDown() {
		jmxConnector.shutdown();
	}

	@Test
	public void testPredicatePushdown() throws Exception {
		for (Node node : nodes) {
			String nodeIdentifier = node.getNodeIdentifier();
			TupleDomain<ColumnHandle> nodeTupleDomain = TupleDomain
					.fromFixedValues(
							ImmutableMap.of(columnHandle,
									NullableValue.of(
											createUnboundedVarcharType(),
											utf8Slice(nodeIdentifier))));
			ConnectorTableLayoutHandle layout = new JmxTableLayoutHandle(
					tableHandle, nodeTupleDomain);

			ConnectorSplitSource splitSource = splitManager.getSplits(
					JmxTransactionHandle.INSTANCE, SESSION, layout,
					new PushDown(null, null, null));
			List<ConnectorSplit> allSplits = getAllSplits(splitSource);

			assertEquals(allSplits.size(), 1);
			assertEquals(allSplits.get(0).getAddresses().size(), 1);
			assertEquals(allSplits.get(0).getAddresses().get(0).getHostText(),
					nodeIdentifier);
		}
	}

	@Test
	public void testNoPredicate() throws Exception {
		ConnectorTableLayoutHandle layout = new JmxTableLayoutHandle(
				tableHandle, TupleDomain.all());
		ConnectorSplitSource splitSource = splitManager.getSplits(
				JmxTransactionHandle.INSTANCE, SESSION, layout,
				new PushDown(null, null, null));
		List<ConnectorSplit> allSplits = getAllSplits(splitSource);
		assertEquals(allSplits.size(), nodes.size());

		Set<String> actualNodes = nodes.stream().map(Node::getNodeIdentifier)
				.collect(toSet());
		Set<String> expectedNodes = new HashSet<>();
		for (ConnectorSplit split : allSplits) {
			List<HostAddress> addresses = split.getAddresses();
			assertEquals(addresses.size(), 1);
			expectedNodes.add(addresses.get(0).getHostText());
		}
		assertEquals(actualNodes, expectedNodes);
	}

	@Test
	public void testRecordSetProvider() throws Exception {
		for (SchemaTableName schemaTableName : metadata.listTables(SESSION,
				JMX_SCHEMA_NAME)) {
			RecordSet recordSet = getRecordSet(schemaTableName);
			try (RecordCursor cursor = recordSet.cursor()) {
				while (cursor.advanceNextPosition()) {
					for (int i = 0; i < recordSet.getColumnTypes()
							.size(); i++) {
						cursor.isNull(i);
					}
				}
			}
		}
	}

	@Test
	public void testHistoryRecordSetProvider() throws Exception {
		for (SchemaTableName schemaTableName : metadata.listTables(SESSION,
				HISTORY_SCHEMA_NAME)) {
			// wait for at least two samples
			List<Long> timeStamps = ImmutableList.of();
			for (int waited = 0; waited < TIMEOUT_TIME; waited += SLEEP_TIME) {
				RecordSet recordSet = getRecordSet(schemaTableName);
				timeStamps = readTimeStampsFrom(recordSet);
				if (timeStamps.size() >= 2) {
					break;
				}
				Thread.sleep(SLEEP_TIME);
			}
			assertTrue(timeStamps.size() >= 2);

			// we don't have equality check here because JmxHistoryDumper
			// scheduling can lag
			assertTrue(timeStamps.get(1) - timeStamps.get(0) >= JMX_STATS_DUMP
					.toMillis());
		}
	}

	private List<Long> readTimeStampsFrom(RecordSet recordSet) {
		ImmutableList.Builder<Long> result = ImmutableList.builder();
		try (RecordCursor cursor = recordSet.cursor()) {
			while (cursor.advanceNextPosition()) {
				for (int i = 0; i < recordSet.getColumnTypes().size(); i++) {
					cursor.isNull(i);
				}
				if (cursor.isNull(0)) {
					return result.build();
				}
				assertTrue(recordSet.getColumnTypes()
						.get(0) instanceof TimestampType);
				result.add(cursor.getLong(0));
			}
		}
		return result.build();
	}

	private RecordSet getRecordSet(SchemaTableName schemaTableName)
			throws Exception {
		JmxTableHandle tableHandle = metadata.getTableHandle(SESSION,
				schemaTableName);
		List<ColumnHandle> columnHandles = ImmutableList.copyOf(
				metadata.getColumnHandles(SESSION, tableHandle).values());

		ConnectorTableLayoutHandle layout = new JmxTableLayoutHandle(
				tableHandle, TupleDomain.all());
		ConnectorSplitSource splitSource = splitManager.getSplits(
				JmxTransactionHandle.INSTANCE, SESSION, layout,
				new PushDown(null, null, null));
		List<ConnectorSplit> allSplits = getAllSplits(splitSource);
		assertEquals(allSplits.size(), nodes.size());
		ConnectorSplit split = allSplits.get(0);

		return recordSetProvider.getRecordSet(JmxTransactionHandle.INSTANCE,
				SESSION, split, columnHandles);
	}

	private static List<ConnectorSplit> getAllSplits(
			ConnectorSplitSource splitSource)
			throws InterruptedException, ExecutionException {
		ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
		while (!splitSource.isFinished()) {
			List<ConnectorSplit> batch = splitSource.getNextBatch(1000).get();
			splits.addAll(batch);
		}
		return splits.build();
	}

	private class TestingNodeManager implements NodeManager {
		@Override
		public Set<Node> getNodes(NodeState state) {
			return nodes;
		}

		@Override
		public Set<Node> getActiveDatasourceNodes(String datasourceName) {
			return nodes;
		}

		@Override
		public Node getCurrentNode() {
			return localNode;
		}

		@Override
		public Set<Node> getCoordinators() {
			return ImmutableSet.of(localNode);
		}

		@Override
		public Set<Node> getWeiwomanagers() {
			return ImmutableSet.of(localNode);
		}
	}

	private static class TestingNode implements Node {
		private final String hostname;

		public TestingNode(String hostname) {
			this.hostname = hostname;
		}

		@Override
		public HostAddress getHostAndPort() {
			return HostAddress.fromParts(hostname, 8080);
		}

		@Override
		public URI getHttpUri() {
			return URI.create(format("http://%s:8080", hostname));
		}

		@Override
		public String getNodeIdentifier() {
			return hostname;
		}
	}
}
