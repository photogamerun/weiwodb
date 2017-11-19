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

import static com.facebook.presto.lucene.HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.lucene.LuceneHiveTableProperties.getBucketProperty;
import static com.facebook.presto.lucene.LuceneHiveTableProperties.getLuceneHiveStorageFormat;
import static com.facebook.presto.lucene.Types.checkType;
import static com.facebook.presto.lucene.hive.metastore.HiveType.toHiveType;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;
import com.facebook.presto.lucene.hive.metastore.HiveType;
import com.facebook.presto.lucene.hive.metastore.LuceneMetastore;
import com.facebook.presto.lucene.hive.metastore.LuceneViewNotSupportedException;
import com.facebook.presto.lucene.util.LuceneColumnUtil;
import com.facebook.presto.lucene.util.LuceneUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.TupleDomain.ColumnDomain;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;

public class LuceneMetadata implements ConnectorMetadata {

	private static final Logger log = Logger.get(LuceneMetadata.class);

	private LuceneMetastore client;
	private String connectorId;
	private final TypeManager typeManager;
	private final ZkClientFactory zkClientFactory;
	private final LuceneConfig config;
	private LoadingCache<String, List<String>> tableCache;
	private long expireTime = 1000l * 60l * 60l * 24l;
	private long refreshTime = 1000l * 60l * 60l;
	private WeiwoDBHdfsConfiguration hdfsConfigration;
	private FileSystem fs;

	@Inject
	public LuceneMetadata(LuceneMetastore client, LuceneConnectorId connectorId,
			TypeManager typeManager, ZkClientFactory zkClientFactory,
			LuceneConfig config, WeiwoDBHdfsConfiguration hdfsConfigration) {
		ExecutorService executor = newFixedThreadPool(
				config.getMetaRefreshMaxThreads(),
				daemonThreadsNamed("zookeeper-metaStore"
						+ connectorId.toString() + "-%s"));
		this.connectorId = requireNonNull(connectorId, "connectorId is null")
				.toString();
		this.client = requireNonNull(client, "client is null");
		this.hdfsConfigration = requireNonNull(hdfsConfigration,
				"hdfsConfigration is null");
		this.typeManager = requireNonNull(typeManager, "typeManager is null");
		this.zkClientFactory = requireNonNull(zkClientFactory,
				"zkClientFactory is null");
		this.config = requireNonNull(config, "config is null");
		tableCache = CacheBuilder.newBuilder()
				.expireAfterWrite(expireTime, MILLISECONDS)
				.refreshAfterWrite(refreshTime, MILLISECONDS)
				.build(asyncReloading(new CacheLoader<String, List<String>>() {
					@Override
					public List<String> load(String key) throws Exception {
						return listTables(key);
					}
				}, executor));
		try {
			this.fs = FileSystem.get(hdfsConfigration.getConfiguration());
		} catch (IOException e) {
			log.error("init hdfs file system fail ", e);
			throw Throwables.propagate(e);
		}
	}

	private void renameIndex(SchemaTableName oldSchema,
			SchemaTableName newSchema) throws IOException {
		String oldParentPath = Joiner
				.on(WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR)
				.join(config.getDataPath(), oldSchema.getSchemaName());

		String newParentPath = Joiner
				.on(WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR)
				.join(config.getDataPath(), newSchema.getSchemaName());

		fs.rename(new Path(oldParentPath, oldSchema.getTableName()),
				new Path(newParentPath, newSchema.getTableName()));
	}

	private void dropIndex(SchemaTableName tableSchema) throws IOException {
		String parentPath = Joiner.on(WeiwoDBConfigureKeys.HDFS_PATH_SEPARATOR)
				.join(config.getZkPath(), tableSchema.getSchemaName());
		fs.deleteOnExit(new Path(parentPath, tableSchema.getTableName()));
	}

	@Override
	public List<String> listSchemaNames(ConnectorSession session) {
		return client.getAllDatabases();
	}

	@Override
	public ConnectorTableHandle getTableHandle(ConnectorSession session,
			SchemaTableName tableName) {
		requireNonNull(tableName, "tableName is null");
		if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
			return null;
		}

		if (!client
				.getTable(tableName.getSchemaName(), tableName.getTableName())
				.isPresent()) {
			return null;
		}

		return new LuceneTableHandle(connectorId, tableName, connectorId,
				tableName.getSchemaName(), tableName.getTableName());
	}

	public static NullableValue parsePartitionValue(
			LuceneColumnHandle columnHandle, String value, Type type) {
		requireNonNull(columnHandle, "columnHandle is null");
		requireNonNull(value, "value is null");
		requireNonNull(type, "type is null");

		if (type instanceof VarcharType) {
			return NullableValue.of(type, varcharPartitionKey(value,
					columnHandle.getColumnName(), type));
		}

		throw new PrestoException(NOT_SUPPORTED,
				format("Unsupported Type [%s] for partition: %s", type,
						columnHandle.getColumnName()));
	}

	public static Slice varcharPartitionKey(String value, String name,
			Type columnType) {
		Slice partitionKey = Slices.utf8Slice(value);
		VarcharType varcharType = checkType(columnType, VarcharType.class,
				"columnType");
		if (SliceUtf8.countCodePoints(partitionKey) > varcharType.getLength()) {
			throw new PrestoException(NOT_SUPPORTED,
					format("Invalid partition value '%s' for %s partition key: %s",
							value, columnType.toString(), name));
		}
		return partitionKey;
	}

	private List<TupleDomain<ColumnHandle>> getPartitions(
			LuceneColumnHandle column, LuceneTableHandle tableHandle) {
		try {
			List<String> partitionValues = new ArrayList<>();
			Path tablePath = new Path(new Path(config.getDataPath()),
					tableHandle.getSchemaName() + LuceneUtils.FILE_SEP
							+ tableHandle.getTableName());
			FileSystem fs = FileSystem.get(hdfsConfigration.getConfiguration());
			if (fs.exists(tablePath)) {
				FileStatus[] files = fs.listStatus(tablePath);
				if (files != null) {
					for (FileStatus file : files) {
						partitionValues.add(file.getPath().getName());
					}
				}
			}
			return partitionValues.stream()
					.map(partitionValue -> buildValue(column, partitionValue))
					.collect(toList());
		} catch (IOException e) {
			throw Throwables.propagate(e);
		}
	}

	private TupleDomain<ColumnHandle> buildValue(LuceneColumnHandle column,
			String partitionValue) {
		ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap
				.builder();
		builder.put(column, parsePartitionValue(column, partitionValue,
				typeManager.getType(column.getTypeSignature())));
		return TupleDomain.fromFixedValues(builder.build());
	}

	@Override
	public List<ConnectorTableLayoutResult> getTableLayouts(
			ConnectorSession session, ConnectorTableHandle table,
			Constraint<ColumnHandle> constraint,
			Optional<Set<ColumnHandle>> desiredColumns) {
		LuceneTableHandle tableHandle = checkType(table,
				LuceneTableHandle.class, "table");
		List<LuceneColumnHandle> partitionColumns = new ArrayList<>();
		List<ColumnDomain<ColumnHandle>> columns = constraint.getSummary()
				.getColumnDomains().get();
		columns.forEach(col -> {
			LuceneColumnHandle c = checkType(col.getColumn(),
					LuceneColumnHandle.class, "columnHandle");
			if (LuceneColumnUtil.isPartitionKey(c.getColumnName())) {
				partitionColumns.add(c);
			}
		});
		if (partitionColumns.size() <= 0) {
			Map<String, ColumnHandle> result = getColumnHandles(session, table);
			result.values().forEach(col -> {
				LuceneColumnHandle c = checkType(col, LuceneColumnHandle.class,
						"columnHandle");
				if (LuceneColumnUtil.isPartitionKey(c.getColumnName())) {
					partitionColumns.add(c);
				}
			});
		}

		Optional<DiscretePredicates> discretePredicates = Optional.empty();
		if (partitionColumns.size() > 0) {
			Domain v = constraint.getSummary().getDomains().get()
					.get(partitionColumns.get(0));
			if (v == null) {
				discretePredicates = Optional.of(new DiscretePredicates(
						ImmutableList.of(partitionColumns.get(0)),
						getPartitions(partitionColumns.get(0), tableHandle)));
			} else {
				Map<ColumnHandle, Domain> map = new HashMap<>();
				map.put(partitionColumns.get(0), v);
				discretePredicates = Optional.of(new DiscretePredicates(
						ImmutableList.of(partitionColumns.get(0)),
						ImmutableList.of(TupleDomain.withColumnDomains(map))));
			}
		}
		ConnectorTableLayout layout = new ConnectorTableLayout(
				new LuceneTableLayoutHandle(tableHandle,
						constraint.getSummary()),
				Optional.empty(), TupleDomain.all(), Optional.empty(),
				Optional.empty(), discretePredicates, emptyList());
		return ImmutableList.of(new ConnectorTableLayoutResult(layout,
				constraint.getSummary()));
	}

	@Override
	public ConnectorTableLayout getTableLayout(ConnectorSession session,
			ConnectorTableLayoutHandle handle) {
		return new ConnectorTableLayout(handle, Optional.empty(),
				TupleDomain.all(), Optional.empty(), Optional.empty(),
				Optional.empty(), emptyList());
	}

	@Override
	public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
			ConnectorTableHandle table) {
		requireNonNull(table, "tableHandle is null");
		SchemaTableName tableName = schemaTableName(table);
		return getTableMetadata(tableName);
	}

	public static SchemaTableName schemaTableName(
			ConnectorTableHandle tableHandle) {
		return checkType(tableHandle, LuceneTableHandle.class, "tableHandle")
				.getSchemaTableName();
	}

	public static List<LuceneColumnHandle> hiveColumnHandles(String connectorId,
			Table table) {
		ImmutableList.Builder<LuceneColumnHandle> columns = ImmutableList
				.builder();

		// add the data fields first
		columns.addAll(getNonPartitionKeyColumnHandles(connectorId, table));

		// add the partition keys last (like Hive does)
		// columns.addAll(getPartitionKeyColumnHandles(connectorId, table));

		return columns.build();
	}

	public static List<LuceneColumnHandle> getNonPartitionKeyColumnHandles(
			String connectorId, Table table) {
		ImmutableList.Builder<LuceneColumnHandle> columns = ImmutableList
				.builder();

		Map<String, String> columnTypes = LuceneUtils
				.getLuceneColumnTypes(table.getParameters());
		int hiveColumnIndex = 0;
		for (FieldSchema field : table.getSd().getCols()) {
			// ignore unsupported types rather than failing
			HiveType hiveType = HiveType.valueOf(field.getType());
			// Hive 中的 float 字段在 0.151 presto 中支持的不够，所以发现float 后强制转换成double 计算
			if (Objects.equals(HiveType.HIVE_FLOAT, hiveType)) {
				if (hiveType.isSupportedType()) {
					columns.add(new LuceneColumnHandle(connectorId,
							field.getName(), hiveType, hiveColumnIndex,
							RealType.REAL.getTypeSignature(),
							columnTypes.get(field.getName())));
					hiveColumnIndex++;
				}
			} else {
				if (hiveType.isSupportedType()) {
					columns.add(new LuceneColumnHandle(connectorId,
							field.getName(), hiveType, hiveColumnIndex,
							hiveType.getTypeSignature(),
							columnTypes.get(field.getName())));
					hiveColumnIndex++;
				}
			}
		}

		return columns.build();
	}

	// public static List<LuceneColumnHandle>
	// getPartitionKeyColumnHandles(String connectorId, Table table)
	// {
	// ImmutableList.Builder<LuceneColumnHandle> columns =
	// ImmutableList.builder();
	//
	// List<FieldSchema> partitionKeys = table.getPartitionKeys();
	// for (FieldSchema field : partitionKeys) {
	// HiveType hiveType = HiveType.valueOf(field.getType());
	// if (!hiveType.isSupportedType()) {
	// throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type %s
	// found in partition keys of table %s.%s", hiveType, table.getDbName(),
	// table.getTableName()));
	// }
	// columns.add(new LuceneColumnHandle(connectorId, field.getName(),
	// hiveType, -1, hiveType.getTypeSignature()));
	// }
	//
	// return columns.build();
	// }

	private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName) {
		Optional<Table> table = client.getTable(tableName.getSchemaName(),
				tableName.getTableName());
		if (!table.isPresent() || table.get().getTableType()
				.equals(TableType.VIRTUAL_VIEW.name())) {
			throw new TableNotFoundException(tableName);
		}

		List<LuceneColumnHandle> result = hiveColumnHandles(connectorId,
				table.get());

		ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
		for (LuceneColumnHandle columnHandle : result) {
			columns.add(new ColumnMetadata(columnHandle.getColumnName(),
					typeManager.getType(columnHandle.getTypeSignature())));
		}

		ImmutableMap.Builder<String, Object> properties = ImmutableMap
				.builder();
		properties.putAll(table.get().getParameters());

		return new ConnectorTableMetadata(tableName, columns.build(),
				properties.build(), table.get().getOwner(), false);
	}

	public static String annotateColumnComment(String comment,
			boolean partitionKey) {
		comment = nullToEmpty(comment).trim();
		if (partitionKey) {
			if (comment.isEmpty()) {
				comment = "Partition Key";
			} else {
				comment = "Partition Key: " + comment;
			}
		}
		return emptyToNull(comment);
	}

	@Override
	public List<SchemaTableName> listTables(ConnectorSession session,
			String schemaNameOrNull) {
		ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList
				.builder();
		for (String schemaName : listSchemas(session, schemaNameOrNull)) {
			try {
				for (String tableName : tableCache.get(schemaName)) {
					tableNames.add(new SchemaTableName(schemaName, tableName));
				}
			} catch (ExecutionException e) {
				throw Throwables.propagate(e);
			}
		}
		return tableNames.build();
	}

	private List<String> listTables(String schemaName) {
		if (zkClientFactory.getZkClient().exists(config.getZkPath()
				+ WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
				+ WeiwoDBConfigureKeys.WEIWO_TABLE_PATH
				+ WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + schemaName)) {
			return zkClientFactory.getZkClient().getChildren(config.getZkPath()
					+ WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
					+ WeiwoDBConfigureKeys.WEIWO_TABLE_PATH
					+ WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + schemaName);
		} else {
			return new ArrayList<>();
		}
	}

	private List<String> listSchemas(ConnectorSession session,
			String schemaNameOrNull) {
		if (schemaNameOrNull == null) {
			return listSchemaNames(session);
		}
		return ImmutableList.of(schemaNameOrNull);
	}

	@Override
	public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
			ConnectorTableHandle tableHandle) {
		SchemaTableName tableName = schemaTableName(tableHandle);
		Optional<Table> table = client.getTable(tableName.getSchemaName(),
				tableName.getTableName());
		if (!table.isPresent()) {
			throw new TableNotFoundException(tableName);
		}
		ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap
				.builder();
		for (LuceneColumnHandle columnHandle : hiveColumnHandles(connectorId,
				table.get())) {
			columnHandles.put(columnHandle.getColumnName(), columnHandle);
		}
		return columnHandles.build();
	}

	@Override
	public ColumnMetadata getColumnMetadata(ConnectorSession session,
			ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
		checkType(tableHandle, LuceneTableHandle.class, "tableHandle");
		return checkType(columnHandle, LuceneColumnHandle.class, "columnHandle")
				.getColumnMetadata(typeManager);
	}

	private List<SchemaTableName> listTables(ConnectorSession session,
			SchemaTablePrefix prefix) {
		if (prefix.getSchemaName() == null || prefix.getTableName() == null) {
			return listTables(session, prefix.getSchemaName());
		}
		return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(),
				prefix.getTableName()));
	}

	@SuppressWarnings("TryWithIdenticalCatches")
	@Override
	public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
			ConnectorSession session, SchemaTablePrefix prefix) {
		requireNonNull(prefix, "prefix is null");
		ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap
				.builder();
		for (SchemaTableName tableName : listTables(session, prefix)) {
			try {
				columns.put(tableName,
						getTableMetadata(tableName).getColumns());
			} catch (LuceneViewNotSupportedException e) {
				// view is not supported
			} catch (TableNotFoundException e) {
				// table disappeared during listing operation
			}
		}
		return columns.build();
	}

	@Override
	public void renameTable(ConnectorSession session,
			ConnectorTableHandle tableHandle, SchemaTableName newTableName) {
		// TODO Auto-generated method stub
		LuceneTableHandle handle = checkType(tableHandle,
				LuceneTableHandle.class, "tableHandle");
		SchemaTableName tableName = schemaTableName(tableHandle);
		Optional<Table> source = client.getTable(handle.getSchemaName(),
				handle.getTableName());
		if (!source.isPresent()) {
			throw new TableNotFoundException(tableName);
		}
		Table table = source.get();

		table.setDbName(newTableName.getSchemaName());
		table.setTableName(newTableName.getTableName());
		try {
			this.renameTableOnZk(tableName, newTableName);
			this.renameIndex(tableName, newTableName);
			client.alterTable(handle.getSchemaName(), handle.getTableName(),
					table);
		} catch (IOException e) {
			throw new TableNotFoundException(tableName);
		}

	}

	@Override
	public void dropTable(ConnectorSession session,
			ConnectorTableHandle tableHandle) {
		LuceneTableHandle handle = checkType(tableHandle,
				LuceneTableHandle.class, "tableHandle");
		SchemaTableName tableName = schemaTableName(tableHandle);

		Optional<Table> target = client.getTable(handle.getSchemaName(),
				handle.getTableName());
		if (!target.isPresent()) {
			throw new TableNotFoundException(tableName);
		}
		client.dropTable(handle.getSchemaName(), handle.getTableName());
		try {
			this.dropIndex(tableName);
		} catch (IOException e) {
			throw new TableNotFoundException(tableName);
		}
		this.dropTableOnZk(tableName);

	}

	public void dropTableOnZk(SchemaTableName tableName) {
		String path = Joiner.on(WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR).join(
				config.getZkPath(), WeiwoDBConfigureKeys.WEIWO_TABLE_PATH,
				tableName.getSchemaName(), tableName.getTableName());
		if (zkClientFactory.getZkClient().exists(path)) {
			zkClientFactory.getZkClient().delete(path);
		} else {
			throw new PrestoException(INVALID_TABLE_PROPERTY,
					"Table not exist on zk how to drop: "
							+ tableName.getSchemaName() + "."
							+ tableName.getTableName());
		}
		tableCache.refresh(tableName.getSchemaName());
	}

	@Override
	public void createTable(ConnectorSession session,
			ConnectorTableMetadata tableMetadata) {
		checkArgument(!isNullOrEmpty(tableMetadata.getOwner()),
				"Table owner is null or empty");

		SchemaTableName schemaTableName = tableMetadata.getTable();
		String schemaName = schemaTableName.getSchemaName();
		String tableName = schemaTableName.getTableName();
		List<String> partitionedBy = new ArrayList<>();
		List<HiveColumnHandle> columnHandles = getColumnHandles(connectorId,
				tableMetadata, ImmutableSet.copyOf(partitionedBy));
		String weiwoMapping = (String) tableMetadata.getProperties()
				.get(LuceneUtils.WEIWODB_SCHEMA_MAPPING_KEY);
		String dataLocation = (String) (tableMetadata.getProperties()
				.get(LuceneUtils.WEIWODB_DATA_LOCATION) == null
						? ""
						: tableMetadata.getProperties()
								.get(LuceneUtils.WEIWODB_DATA_LOCATION));
		String storageHandler = (String) tableMetadata.getProperties()
				.get(LuceneUtils.WEIWODB_STORAGE_HANDLER);
		Map<String, String> luceneColumns = LuceneUtils
				.getLuceneColumnTypes(weiwoMapping);
		validateLuceneMapping(columnHandles, luceneColumns);
		LuceneHiveStorageFormat hiveStorageFormat = getLuceneHiveStorageFormat(
				tableMetadata.getProperties());
		Map<String, String> additionalTableParameters = new HashMap<>();
		additionalTableParameters.put(LuceneUtils.WEIWODB_SCHEMA_MAPPING_KEY,
				weiwoMapping);
		additionalTableParameters.put(LuceneUtils.WEIWODB_DATA_LOCATION,
				dataLocation);
		if (storageHandler != null) {
			additionalTableParameters.put(LuceneUtils.WEIWODB_STORAGE_HANDLER,
					storageHandler);
		}
		Optional<HiveBucketProperty> bucketProperty = getBucketProperty(
				tableMetadata.getProperties());
		Path targetPath = new Path(
				getDatabase(client, schemaName).getLocationUri(), tableName);
		log.info("Create table default location = " + targetPath.toString());
		Table table = buildTableObject(schemaName, tableName,
				tableMetadata.getOwner(), columnHandles, hiveStorageFormat,
				partitionedBy, bucketProperty, additionalTableParameters,
				targetPath);
		client.createTable(table);
		createTableOnZk(schemaName, tableName, luceneColumns);
	}

	private static Database getDatabase(LuceneMetastore client,
			String database) {
		return client.getDatabase(database)
				.orElseThrow(() -> new SchemaNotFoundException(database));
	}

	private void createTableOnZk(String schema, String table,
			Map<String, String> luceneColumns) {
		String path = Joiner.on(WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR).join(
				config.getZkPath(), WeiwoDBConfigureKeys.WEIWO_TABLE_PATH,
				schema, table);
		if (zkClientFactory.getZkClient().exists(path)) {
			throw new PrestoException(INVALID_TABLE_PROPERTY,
					"Table exist on zk : " + schema + "." + table);
		} else {
			JSONObject json = new JSONObject();
			json.put(WeiwoDBConfigureKeys.WEIWO_TABLE_LUCENE_SCHEMA_KEY,
					luceneColumns);
			zkClientFactory.getZkClient().createPersistent(path, true);
			zkClientFactory.getZkClient().writeData(path, json.toJSONString());
		}

		tableCache.refresh(schema);
	}

	private void renameTableOnZk(SchemaTableName tableName,
			SchemaTableName newtableName) {
		String path = Joiner.on(WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR).join(
				config.getZkPath(), WeiwoDBConfigureKeys.WEIWO_TABLE_PATH,
				tableName.getSchemaName(), tableName.getTableName());

		String newPath = Joiner.on(WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR).join(
				config.getZkPath(), WeiwoDBConfigureKeys.WEIWO_TABLE_PATH,
				newtableName.getSchemaName(), newtableName.getTableName());

		if (zkClientFactory.getZkClient().exists(path)) {
			String tableDefine = zkClientFactory.getZkClient().readData(path);
			zkClientFactory.getZkClient().delete(path);
			zkClientFactory.getZkClient().createPersistent(newPath, true);
			zkClientFactory.getZkClient().writeData(newPath, tableDefine);
		} else {
			throw new PrestoException(INVALID_TABLE_PROPERTY,
					"Table not exist on zk how to rename: "
							+ tableName.getSchemaName() + "."
							+ tableName.getTableName());
		}
		tableCache.refresh(tableName.getSchemaName());
	}

	private void validateLuceneMapping(List<HiveColumnHandle> columnHandles,
			Map<String, String> luceneColumns) {
		List<String> hiveColumnHandles = columnHandles.stream()
				.map(handle -> handle.getName()).collect(Collectors.toList());
		if (!hiveColumnHandles.containsAll(luceneColumns.keySet())
				&& luceneColumns.keySet().containsAll(hiveColumnHandles)) {
			throw new PrestoException(INVALID_TABLE_PROPERTY,
					"Mapping column must the same as table column.");
		}
		for (String str : luceneColumns.values()) {
			if (WeiwoDBType.UNKNOWN.equals(WeiwoDBType.getType(str))) {
				throw new PrestoException(INVALID_TABLE_PROPERTY,
						"Mapping column type = " + str + " not support.");
			}
		}
	};

	private Table buildTableObject(String schemaName, String tableName,
			String tableOwner, List<HiveColumnHandle> columnHandles,
			LuceneHiveStorageFormat hiveStorageFormat,
			List<String> partitionedBy,
			Optional<HiveBucketProperty> bucketProperty,
			Map<String, String> additionalTableParameters, Path targetPath) {
		Map<String, HiveColumnHandle> columnHandlesByName = Maps
				.uniqueIndex(columnHandles, HiveColumnHandle::getName);
		List<FieldSchema> partitionColumns = partitionedBy.stream()
				.map(columnHandlesByName::get)
				.map(column -> new FieldSchema(column.getName(),
						column.getHiveType().getHiveTypeName(), null))
				.collect(toList());

		Set<String> partitionColumnNames = ImmutableSet.copyOf(partitionedBy);

		boolean sampled = false;
		ImmutableList.Builder<FieldSchema> columns = ImmutableList.builder();
		for (HiveColumnHandle columnHandle : columnHandles) {
			String name = columnHandle.getName();
			String type = columnHandle.getHiveType().getHiveTypeName();
			if (name.equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
				columns.add(new FieldSchema(name, type,
						"Presto sample weight column"));
				sampled = true;
			} else if (!partitionColumnNames.contains(name)) {
				verify(!columnHandle.isPartitionKey(),
						"Column handles are not consistent with partitioned by property");
				columns.add(new FieldSchema(name, type, null));
			} else {
				verify(columnHandle.isPartitionKey(),
						"Column handles are not consistent with partitioned by property");
			}
		}

		Table table = new Table();
		table.setDbName(schemaName);
		table.setTableName(tableName);
		table.setOwner(tableOwner);
		table.setTableType(TableType.MANAGED_TABLE.toString());
		String tableComment = "Created by Presto";
		if (sampled) {
			tableComment = "Sampled table created by Presto. Only query this table from Hive if you understand how Presto implements sampling.";
		}
		table.setParameters(ImmutableMap.<String, String> builder()
				.put("comment", tableComment).putAll(additionalTableParameters)
				.build());
		table.setPartitionKeys(partitionColumns);
		table.setSd(makeStorageDescriptor(tableName, hiveStorageFormat,
				targetPath, columns.build(), bucketProperty));

		PrivilegeGrantInfo allPrivileges = new PrivilegeGrantInfo("all", 0,
				tableOwner, PrincipalType.USER, true);
		table.setPrivileges(new PrincipalPrivilegeSet(
				ImmutableMap.of(tableOwner, ImmutableList.of(allPrivileges)),
				ImmutableMap.of(), ImmutableMap.of()));

		return table;
	}

	private static StorageDescriptor makeStorageDescriptor(String tableName,
			LuceneHiveStorageFormat format, Path targetPath,
			List<FieldSchema> columns,
			Optional<HiveBucketProperty> bucketProperty) {
		SerDeInfo serdeInfo = new SerDeInfo();
		serdeInfo.setName(tableName);
		serdeInfo.setSerializationLib(format.getSerDe());
		serdeInfo.setParameters(ImmutableMap.of());

		StorageDescriptor sd = new StorageDescriptor();
		sd.setLocation(targetPath.toString());
		sd.setCols(columns);
		sd.setSerdeInfo(serdeInfo);
		sd.setInputFormat(format.getInputFormat());
		sd.setOutputFormat(format.getOutputFormat());
		sd.setParameters(ImmutableMap.of());

		bucketProperty.ifPresent(property -> {
			sd.setBucketCols(property.getBucketedBy());
			sd.setNumBuckets(property.getBucketCount());
		});

		return sd;
	}

	private static void validatePartitionColumns(
			ConnectorTableMetadata tableMetadata) {

		List<String> allColumns = tableMetadata.getColumns().stream()
				.map(ColumnMetadata::getName).collect(toList());

		if (!allColumns.contains(LuceneColumnUtil.PARTITION_KEY)) {
			throw new PrestoException(INVALID_TABLE_PROPERTY,
					format("Partition columns %s not present in schema",
							LuceneColumnUtil.PARTITION_KEY));
		}

		if (allColumns.size() == 1) {
			throw new PrestoException(INVALID_TABLE_PROPERTY,
					"Table contains only partition columns");
		}

		if (!allColumns.get(allColumns.size() - 1)
				.equals(LuceneColumnUtil.PARTITION_KEY)) {
			throw new PrestoException(INVALID_TABLE_PROPERTY,
					"Partition keys must be the last columns in the table and in the same order as the table properties: "
							+ LuceneColumnUtil.PARTITION_KEY);
		}
	}

	private static List<HiveColumnHandle> getColumnHandles(String connectorId,
			ConnectorTableMetadata tableMetadata,
			Set<String> partitionColumnNames) {
		validatePartitionColumns(tableMetadata);

		ImmutableList.Builder<HiveColumnHandle> columnHandles = ImmutableList
				.builder();
		int ordinal = 0;
		for (ColumnMetadata column : tableMetadata.getColumns()) {
			columnHandles.add(new HiveColumnHandle(connectorId,
					column.getName(), toHiveType(column.getType()),
					column.getType().getTypeSignature(), ordinal,
					partitionColumnNames.contains(column.getName())));
			ordinal++;
		}
		if (tableMetadata.isSampled()) {
			columnHandles.add(new HiveColumnHandle(connectorId,
					SAMPLE_WEIGHT_COLUMN_NAME, toHiveType(BIGINT),
					BIGINT.getTypeSignature(), ordinal, false));
		}

		return columnHandles.build();
	}

}
