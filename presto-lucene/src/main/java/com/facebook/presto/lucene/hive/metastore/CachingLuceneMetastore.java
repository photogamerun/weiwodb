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
package com.facebook.presto.lucene.hive.metastore;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.USER;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.facebook.presto.lucene.LuceneConfig;
import com.facebook.presto.lucene.LuceneConnectorId;
import com.facebook.presto.lucene.services.security.internals.LucenePrivilegeInfo;
import com.facebook.presto.lucene.services.security.internals.LucenePrivilegeInfo.LucenePrivilege;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;

public class CachingLuceneMetastore implements LuceneMetastore {

	private final CachingLuceneMetastoreStats stats = new CachingLuceneMetastoreStats();
	protected final LuceneMetastoreClientFactory clientProvider;
	private final LoadingCache<String, List<String>> databaseNamesCache;
	private final LoadingCache<String, Optional<Database>> databaseCache;
	private final LoadingCache<String, Optional<List<String>>> tableNamesCache;
	private final LoadingCache<TableName, Optional<Table>> tableCache;
	private final LoadingCache<String, Set<String>> userRolesCache;
	private final LoadingCache<UserTableKey, Set<LucenePrivilegeInfo>> userTablePrivileges;
	@Inject
	public CachingLuceneMetastore(LuceneMetastoreClientFactory clientProvider,
			LuceneConfig config, LuceneConnectorId connector) {
		this(requireNonNull(clientProvider, "clientProvider is null"),
				requireNonNull(config, "config is null").getMetaCacheTtlSec(),
				config.getMetaRefreshIntervalSec(),
				requireNonNull(connector, "connector is null"),
				requireNonNull(config, "config is null"));
	}

	public CachingLuceneMetastore(LuceneMetastoreClientFactory clientProvider,
			int metaCacheTtlSec, int metaRefreshIntervalSec,
			LuceneConnectorId connector, LuceneConfig config) {
		ExecutorService executor = newFixedThreadPool(
				config.getMetaRefreshMaxThreads(),
				daemonThreadsNamed("lucene-hive-metastore-"
						+ connector.toString() + "-%s"));
		this.clientProvider = requireNonNull(clientProvider,
				"hiveCluster is null");

		long expiresAfterWriteMillis = requireNonNull(metaCacheTtlSec,
				"cacheTtl is null") * 1000;
		long refreshMills = requireNonNull(metaRefreshIntervalSec,
				"refreshInterval is null") * 1000;

		databaseNamesCache = CacheBuilder.newBuilder()
				.expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
				.refreshAfterWrite(refreshMills, MILLISECONDS)
				.build(asyncReloading(new CacheLoader<String, List<String>>() {
					@Override
					public List<String> load(String key) throws Exception {
						return loadAllDatabases();
					}
				}, executor));
		databaseCache = CacheBuilder.newBuilder()
				.expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
				.refreshAfterWrite(refreshMills, MILLISECONDS)
				.build(asyncReloading(
						new CacheLoader<String, Optional<Database>>() {
							@Override
							public Optional<Database> load(String databaseName)
									throws Exception {
								return loadDatabase(databaseName);
							}
						}, executor));

		tableNamesCache = CacheBuilder.newBuilder()
				.expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
				.refreshAfterWrite(refreshMills, MILLISECONDS)
				.build(asyncReloading(
						new CacheLoader<String, Optional<List<String>>>() {
							@Override
							public Optional<List<String>> load(
									String databaseName) throws Exception {
								return loadAllTables(databaseName);
							}
						}, executor));

		tableCache = CacheBuilder.newBuilder()
				.expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
				.refreshAfterWrite(refreshMills, MILLISECONDS)
				.build(asyncReloading(
						new CacheLoader<TableName, Optional<Table>>() {
							@Override
							public Optional<Table> load(TableName hiveTableName)
									throws Exception {
								return loadTable(hiveTableName);
							}
						}, executor));

		userRolesCache = CacheBuilder.newBuilder()
				.expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
				.refreshAfterWrite(refreshMills, MILLISECONDS)
				.build(asyncReloading(new CacheLoader<String, Set<String>>() {
					@Override
					public Set<String> load(String user) throws Exception {
						return loadRoles(user);
					}
				}, executor));

		userTablePrivileges = CacheBuilder.newBuilder()
				.expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
				.refreshAfterWrite(refreshMills, MILLISECONDS)
				.build(asyncReloading(
						new CacheLoader<UserTableKey, Set<LucenePrivilegeInfo>>() {
							@Override
							public Set<LucenePrivilegeInfo> load(
									UserTableKey key) throws Exception {
								return loadTablePrivileges(key.getUser(),
										key.getDatabase(), key.getTable());
							}
						}, executor));
	}

	private Set<LucenePrivilegeInfo> loadTablePrivileges(String user,
			String databaseName, String tableName) {
		ImmutableSet.Builder<LucenePrivilegeInfo> privileges = ImmutableSet
				.builder();

		if (isTableOwner(user, databaseName, tableName)) {
			privileges
					.addAll(Arrays.asList(LucenePrivilege.values()).stream()
							.map(hivePrivilege -> new LucenePrivilegeInfo(
									hivePrivilege, true))
							.collect(Collectors.toSet()));
		}
		privileges.addAll(getPrivileges(user, new HiveObjectRef(
				HiveObjectType.TABLE, databaseName, tableName, null, null)));

		return privileges.build();
	}

	private Set<String> loadRoles(String user) throws Exception {
		try {
			return retry().stopOnIllegalExceptions().run("loadRoles",
					stats.getLoadRoles().wrap(() -> {
						try (LuceneMetastoreClient client = clientProvider
								.create()) {
							List<Role> roles = client.listRoles(user, USER);
							if (roles == null) {
								return ImmutableSet.<String> of();
							}
							return ImmutableSet.copyOf(roles.stream()
									.map(Role::getRoleName).collect(toSet()));
						}
					}));
		} catch (TException e) {
			throw new PrestoException(LuceneErrorCode.LUCENE_METASTORE_ERROR,
					e);
		}
	}

	private Set<LucenePrivilegeInfo> getPrivileges(String user,
			HiveObjectRef objectReference) {
		try {
			return retry().stopOnIllegalExceptions().run("getPrivilegeSet",
					stats.getGetPrivilegeSet().wrap(() -> {
						try (LuceneMetastoreClient client = clientProvider
								.create()) {
							ImmutableSet.Builder<LucenePrivilegeInfo> privileges = ImmutableSet
									.builder();
							PrincipalPrivilegeSet privilegeSet = client
									.getPrivilegeSet(objectReference, user,
											null);
							if (privilegeSet != null) {
								Map<String, List<PrivilegeGrantInfo>> userPrivileges = privilegeSet
										.getUserPrivileges();
								if (userPrivileges != null) {
									privileges.addAll(
											toGrants(userPrivileges.get(user)));
								}
								for (List<PrivilegeGrantInfo> rolePrivileges : privilegeSet
										.getRolePrivileges().values()) {
									privileges.addAll(toGrants(rolePrivileges));
								}
								// We do not add the group permissions as Hive
								// does not seem to process these
							}

							return privileges.build();
						}
					}));
		} catch (TException e) {
			throw new PrestoException(LuceneErrorCode.LUCENE_METASTORE_ERROR,
					e);
		} catch (Exception e) {
			if (e instanceof InterruptedException) {
				Thread.currentThread().interrupt();
			}
			throw Throwables.propagate(e);
		}
	}

	private static Set<LucenePrivilegeInfo> toGrants(
			List<PrivilegeGrantInfo> userGrants) {
		if (userGrants == null) {
			return ImmutableSet.of();
		}

		ImmutableSet.Builder<LucenePrivilegeInfo> privileges = ImmutableSet
				.builder();
		for (PrivilegeGrantInfo userGrant : userGrants) {
			privileges.addAll(LucenePrivilegeInfo.parsePrivilege(userGrant));
		}
		return privileges.build();
	}

	@Override
	public Set<LucenePrivilegeInfo> getDatabasePrivileges(String user,
			String databaseName) {
		ImmutableSet.Builder<LucenePrivilegeInfo> privileges = ImmutableSet
				.builder();

		if (isDatabaseOwner(user, databaseName)) {
			privileges
					.addAll(Arrays.asList(LucenePrivilege.values()).stream()
							.map(hivePrivilege -> new LucenePrivilegeInfo(
									hivePrivilege, true))
							.collect(Collectors.toSet()));
		}
		privileges.addAll(getPrivileges(user, new HiveObjectRef(
				HiveObjectType.DATABASE, databaseName, null, null, null)));

		return privileges.build();
	}

	private LuceneRetryDriver retry() {
		return LuceneRetryDriver.retry().exceptionMapper(getExceptionMapper())
				.stopOn(PrestoException.class);
	}

	protected Function<Exception, Exception> getExceptionMapper() {
		return identity();
	}

	private List<String> loadAllDatabases() throws Exception {
		try {
			return retry().stopOnIllegalExceptions().run("getAllDatabases",
					stats.getGetAllDatabases().wrap(() -> {
						try (LuceneMetastoreClient client = clientProvider
								.create()) {
							return client.getAllDatabases();
						}
					}));
		} catch (TException e) {
			throw new PrestoException(LuceneErrorCode.LUCENE_METASTORE_ERROR,
					e);
		}
	}

	private Optional<Database> loadDatabase(String databaseName)
			throws Exception {
		try {
			return retry().stopOn(NoSuchObjectException.class)
					.stopOnIllegalExceptions()
					.run("getDatabase", stats.getGetDatabase().wrap(() -> {
						try (LuceneMetastoreClient client = clientProvider
								.create()) {
							return Optional
									.of(client.getDatabase(databaseName));
						}
					}));
		} catch (NoSuchObjectException e) {
			return Optional.empty();
		} catch (TException e) {
			throw new PrestoException(LuceneErrorCode.LUCENE_METASTORE_ERROR,
					e);
		}
	}

	private Optional<List<String>> loadAllTables(String databaseName)
			throws Exception {
		Callable<List<String>> getAllTables = stats.getGetAllTables()
				.wrap(() -> {
					try (LuceneMetastoreClient client = clientProvider
							.create()) {
						return client.getAllTables(databaseName);
					}
				});

		Callable<Void> getDatabase = stats.getGetDatabase().wrap(() -> {
			try (LuceneMetastoreClient client = clientProvider.create()) {
				client.getDatabase(databaseName);
				return null;
			}
		});

		try {
			return retry().stopOn(NoSuchObjectException.class)
					.stopOnIllegalExceptions().run("getAllTables", () -> {
						List<String> tables = getAllTables.call();
						if (tables.isEmpty()) {
							// Check to see if the database exists
							getDatabase.call();
						}
						return Optional.of(tables);
					});
		} catch (NoSuchObjectException e) {
			return Optional.empty();
		} catch (TException e) {
			throw new PrestoException(LuceneErrorCode.LUCENE_METASTORE_ERROR,
					e);
		}
	}

	private Optional<Table> loadTable(TableName hiveTableName)
			throws Exception {
		try {
			return retry()
					.stopOn(NoSuchObjectException.class,
							LuceneViewNotSupportedException.class)
					.stopOnIllegalExceptions()
					.run("getTable", stats.getGetTable().wrap(() -> {
						try (LuceneMetastoreClient client = clientProvider
								.create()) {
							Table table = client.getTable(
									hiveTableName.getDatabaseName(),
									hiveTableName.getTableName());
							if (table.getTableType()
									.equals(TableType.VIRTUAL_VIEW.name())) {
								throw new LuceneViewNotSupportedException(
										new SchemaTableName(
												hiveTableName.getDatabaseName(),
												hiveTableName.getTableName()));
							}
							return Optional.of(table);
						}
					}));
		} catch (NoSuchObjectException e) {
			return Optional.empty();
		} catch (TException e) {
			throw new PrestoException(LuceneErrorCode.LUCENE_METASTORE_ERROR,
					e);
		}
	}

	@Override
	public void createTable(Table table) {
		try {
			retry().stopOn(AlreadyExistsException.class,
					InvalidObjectException.class, MetaException.class,
					NoSuchObjectException.class).stopOnIllegalExceptions()
					.run("createTable", stats.getCreateTable().wrap(() -> {
						try (LuceneMetastoreClient client = clientProvider
								.create()) {
							client.createTable(table);
						}
						return null;
					}));
		} catch (AlreadyExistsException e) {
			throw new LuceneTableAlreadyExistsException(new SchemaTableName(
					table.getDbName(), table.getTableName()));
		} catch (NoSuchObjectException e) {
			throw new SchemaNotFoundException(table.getDbName());
		} catch (TException e) {
			throw new PrestoException(LuceneErrorCode.LUCENE_METASTORE_ERROR,
					e);
		} catch (Exception e) {
			if (e instanceof InterruptedException) {
				Thread.currentThread().interrupt();
			}
			throw Throwables.propagate(e);
		} finally {
			invalidateTable(table.getDbName(), table.getTableName());
		}
	}

	@Override
	public void dropTable(String databaseName, String tableName) {
		try {
			retry().stopOn(NoSuchObjectException.class)
					.stopOnIllegalExceptions()
					.run("dropTable", stats.getDropTable().wrap(() -> {
						try (LuceneMetastoreClient client = clientProvider
								.create()) {
							client.dropTable(databaseName, tableName, true);
						}
						return null;
					}));
		} catch (NoSuchObjectException e) {
			throw new TableNotFoundException(
					new SchemaTableName(databaseName, tableName));
		} catch (TException e) {
			throw new PrestoException(LuceneErrorCode.LUCENE_METASTORE_ERROR,
					e);
		} catch (Exception e) {
			if (e instanceof InterruptedException) {
				Thread.currentThread().interrupt();
			}
			throw Throwables.propagate(e);
		} finally {
			invalidateTable(databaseName, tableName);
		}
	}

	@Override
	public void alterTable(String databaseName, String tableName, Table table) {
		try {
			retry().stopOn(InvalidOperationException.class, MetaException.class)
					.stopOnIllegalExceptions()
					.run("alterTable", stats.getAlterTable().wrap(() -> {
						try (LuceneMetastoreClient client = clientProvider
								.create()) {
							Optional<Table> source = loadTable(
									new TableName(databaseName, tableName));
							if (!source.isPresent()) {
								throw new TableNotFoundException(
										new SchemaTableName(databaseName,
												tableName));
							}
							client.alterTable(databaseName, tableName, table);
						}
						return null;
					}));
		} catch (NoSuchObjectException e) {
			throw new TableNotFoundException(
					new SchemaTableName(databaseName, tableName));
		} catch (InvalidOperationException | MetaException e) {
			throw Throwables.propagate(e);
		} catch (TException e) {
			throw new PrestoException(LuceneErrorCode.LUCENE_METASTORE_ERROR,
					e);
		} catch (Exception e) {
			if (e instanceof InterruptedException) {
				Thread.currentThread().interrupt();
			}
			throw Throwables.propagate(e);
		} finally {
			invalidateTable(databaseName, tableName);
			invalidateTable(table.getDbName(), table.getTableName());
		}
	}

	@Override
	public void flushCache() {
		databaseNamesCache.invalidateAll();
		tableNamesCache.invalidateAll();
		databaseCache.invalidateAll();
		tableCache.invalidateAll();
	}

	@Override
	public List<String> getAllDatabases() {
		return get(databaseNamesCache, "");
	}

	@Override
	public Optional<List<String>> getAllTables(String databaseName) {
		return get(tableNamesCache, databaseName);
	}

	@Override
	public Optional<Database> getDatabase(String databaseName) {
		return get(databaseCache, databaseName);
	}

	@Override
	public Optional<Table> getTable(String databaseName, String tableName) {
		return get(tableCache, TableName.table(databaseName, tableName));
	}

	private static <K, V> V get(LoadingCache<K, V> cache, K key) {
		try {
			return cache.get(key);
		} catch (ExecutionException | UncheckedExecutionException
				| ExecutionError e) {
			throw Throwables.propagate(e.getCause());
		}
	}

	private static class TableName {
		private final String databaseName;
		private final String tableName;

		private TableName(String databaseName, String tableName) {
			this.databaseName = databaseName;
			this.tableName = tableName;
		}

		public static TableName table(String databaseName, String tableName) {
			return new TableName(databaseName, tableName);
		}

		public String getDatabaseName() {
			return databaseName;
		}

		public String getTableName() {
			return tableName;
		}

		@Override
		public String toString() {
			return toStringHelper(this).add("databaseName", databaseName)
					.add("tableName", tableName).toString();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			TableName other = (TableName) o;
			return Objects.equals(databaseName, other.databaseName)
					&& Objects.equals(tableName, other.tableName);
		}

		@Override
		public int hashCode() {
			return Objects.hash(databaseName, tableName);
		}
	}

	protected void invalidateTable(String databaseName, String tableName) {
		tableCache.invalidate(new TableName(databaseName, tableName));
		tableNamesCache.invalidate(databaseName);
	}

	@Override
	public Set<String> getRoles(String user) {
		return get(userRolesCache, user);
	}

	@Override
	public Set<LucenePrivilegeInfo> getTablePrivileges(String user,
			String databaseName, String tableName) {
		return get(userTablePrivileges,
				new UserTableKey(user, tableName, databaseName));
	}

	private static class UserTableKey {
		private final String user;
		private final String database;
		private final String table;

		public UserTableKey(String user, String table, String database) {
			this.user = requireNonNull(user, "principalName is null");
			this.table = requireNonNull(table, "table is null");
			this.database = requireNonNull(database, "database is null");
		}

		public String getUser() {
			return user;
		}

		public String getDatabase() {
			return database;
		}

		public String getTable() {
			return table;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			UserTableKey that = (UserTableKey) o;
			return Objects.equals(user, that.user)
					&& Objects.equals(table, that.table)
					&& Objects.equals(database, that.database);
		}

		@Override
		public int hashCode() {
			return Objects.hash(user, table, database);
		}

		@Override
		public String toString() {
			return toStringHelper(this).add("principalName", user)
					.add("table", table).add("database", database).toString();
		}
	}

}
