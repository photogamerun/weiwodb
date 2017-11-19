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
package com.facebook.presto.raptor.metadata;

import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.shardInfo;
import static com.facebook.presto.raptor.util.Types.checkType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.base.Ticker.systemTicker;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.raptor.NodeSupplier;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.RaptorConnectorId;
import com.facebook.presto.raptor.RaptorMetadata;
import com.facebook.presto.raptor.RaptorNodeSupplier;
import com.facebook.presto.raptor.RaptorSplitManager;
import com.facebook.presto.raptor.RaptorTableHandle;
import com.facebook.presto.raptor.RaptorTableLayoutHandle;
import com.facebook.presto.raptor.RaptorTransactionHandle;
import com.facebook.presto.raptor.util.DaoSupplier;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.pushdown.PushDown;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

@Test(singleThreaded = true)
public class TestRaptorSplitManager {
	private static final JsonCodec<ShardInfo> SHARD_INFO_CODEC = jsonCodec(
			ShardInfo.class);
	private static final JsonCodec<ShardDelta> SHARD_DELTA_CODEC = jsonCodec(
			ShardDelta.class);
	private static final ConnectorTableMetadata TEST_TABLE = TableMetadataBuilder
			.tableMetadataBuilder("demo", "test_table")
			.column("ds", createVarcharType(10))
			.column("foo", createVarcharType(10))
			.column("bar", BigintType.BIGINT).build();

	private Handle dummyHandle;
	private File temporary;
	private RaptorMetadata metadata;
	private RaptorSplitManager raptorSplitManager;
	private ConnectorTableHandle tableHandle;
	private ShardManager shardManager;
	private long tableId;

	@BeforeMethod
	public void setup() throws Exception {
		TypeRegistry typeRegistry = new TypeRegistry();
		DBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
		dbi.registerMapper(new TableColumn.Mapper(typeRegistry));
		dummyHandle = dbi.open();
		temporary = createTempDir();
		AssignmentLimiter assignmentLimiter = new AssignmentLimiter(
				ImmutableSet::of, systemTicker(), new MetadataConfig());
		shardManager = new DatabaseShardManager(dbi,
				new DaoSupplier<>(dbi, ShardDao.class), ImmutableSet::of,
				assignmentLimiter, systemTicker(), new Duration(0, MINUTES));
		InMemoryNodeManager nodeManager = new InMemoryNodeManager();
		RaptorNodeSupplier nodeSupplier = new RaptorNodeSupplier(nodeManager,
				new RaptorConnectorId("raptor"));

		String nodeName = UUID.randomUUID().toString();
		nodeManager.addNode("raptor", new PrestoNode(nodeName,
				new URI("http://127.0.0.1/"), NodeVersion.UNKNOWN));

		RaptorConnectorId connectorId = new RaptorConnectorId("raptor");
		metadata = new RaptorMetadata(connectorId.toString(), dbi, shardManager,
				SHARD_INFO_CODEC, SHARD_DELTA_CODEC);

		metadata.createTable(SESSION, TEST_TABLE);
		tableHandle = metadata.getTableHandle(SESSION, TEST_TABLE.getTable());

		List<ShardInfo> shards = ImmutableList.<ShardInfo> builder()
				.add(shardInfo(UUID.randomUUID(), nodeName))
				.add(shardInfo(UUID.randomUUID(), nodeName))
				.add(shardInfo(UUID.randomUUID(), nodeName))
				.add(shardInfo(UUID.randomUUID(), nodeName)).build();

		tableId = checkType(tableHandle, RaptorTableHandle.class, "tableHandle")
				.getTableId();

		List<ColumnInfo> columns = metadata
				.getColumnHandles(SESSION, tableHandle).values().stream()
				.map(handle -> checkType(handle, RaptorColumnHandle.class,
						"columnHandle"))
				.map(ColumnInfo::fromHandle).collect(toList());

		long transactionId = shardManager.beginTransaction();
		shardManager.commitShards(transactionId, tableId, columns, shards,
				Optional.empty());

		raptorSplitManager = new RaptorSplitManager(connectorId, nodeSupplier,
				shardManager, false);
	}

	@AfterMethod
	public void teardown() {
		dummyHandle.close();
		deleteRecursively(temporary);
	}

	@Test
	public void testSanity() throws InterruptedException {
		List<ConnectorTableLayoutResult> layouts = metadata.getTableLayouts(
				SESSION, tableHandle, Constraint.alwaysTrue(),
				Optional.empty());
		assertEquals(layouts.size(), 1);
		ConnectorTableLayoutResult layout = getOnlyElement(layouts);
		assertInstanceOf(layout.getTableLayout().getHandle(),
				RaptorTableLayoutHandle.class);

		ConnectorSplitSource splitSource = getSplits(raptorSplitManager,
				layout);
		int splitCount = 0;
		while (!splitSource.isFinished()) {
			splitCount += getFutureValue(splitSource.getNextBatch(1000)).size();
		}
		assertEquals(splitCount, 4);
	}

	@Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "No host for shard .* found: \\[\\]")
	public void testNoHostForShard() throws InterruptedException {
		deleteShardNodes();

		ConnectorTableLayoutResult layout = getOnlyElement(
				metadata.getTableLayouts(SESSION, tableHandle,
						Constraint.alwaysTrue(), Optional.empty()));
		ConnectorSplitSource splitSource = getSplits(raptorSplitManager,
				layout);
		getFutureValue(splitSource.getNextBatch(1000));
	}

	@Test
	public void testAssignRandomNodeWhenBackupAvailable()
			throws InterruptedException, URISyntaxException {
		InMemoryNodeManager nodeManager = new InMemoryNodeManager();
		RaptorConnectorId connectorId = new RaptorConnectorId("raptor");
		NodeSupplier nodeSupplier = new RaptorNodeSupplier(nodeManager,
				connectorId);
		PrestoNode node = new PrestoNode(UUID.randomUUID().toString(),
				new URI("http://127.0.0.1/"), NodeVersion.UNKNOWN);
		nodeManager.addNode(connectorId.toString(), node);
		RaptorSplitManager raptorSplitManagerWithBackup = new RaptorSplitManager(
				connectorId, nodeSupplier, shardManager, true);

		deleteShardNodes();

		ConnectorTableLayoutResult layout = getOnlyElement(
				metadata.getTableLayouts(SESSION, tableHandle,
						Constraint.alwaysTrue(), Optional.empty()));
		ConnectorSplitSource partitionSplit = getSplits(
				raptorSplitManagerWithBackup, layout);
		List<ConnectorSplit> batch = getFutureValue(
				partitionSplit.getNextBatch(1), PrestoException.class);
		assertEquals(getOnlyElement(getOnlyElement(batch).getAddresses()),
				node.getHostAndPort());
	}

	@Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "No nodes available to run query")
	public void testNoNodes() throws InterruptedException, URISyntaxException {
		deleteShardNodes();

		RaptorSplitManager raptorSplitManagerWithBackup = new RaptorSplitManager(
				new RaptorConnectorId("fbraptor"), ImmutableSet::of,
				shardManager, true);
		ConnectorTableLayoutResult layout = getOnlyElement(
				metadata.getTableLayouts(SESSION, tableHandle,
						Constraint.alwaysTrue(), Optional.empty()));
		ConnectorSplitSource splitSource = getSplits(
				raptorSplitManagerWithBackup, layout);
		getFutureValue(splitSource.getNextBatch(1000), PrestoException.class);
	}

	private void deleteShardNodes() {
		dummyHandle.execute("DELETE FROM shard_nodes");
		dummyHandle.execute(format("UPDATE %s SET node_ids = ''",
				shardIndexTable(tableId)));
	}

	private static ConnectorSplitSource getSplits(
			RaptorSplitManager splitManager,
			ConnectorTableLayoutResult layout) {
		ConnectorTransactionHandle transaction = new RaptorTransactionHandle();
		return splitManager.getSplits(transaction, SESSION,
				layout.getTableLayout().getHandle(), new PushDown(null, null,null));
	}
}
