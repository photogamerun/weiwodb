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

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Client;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class LuceneHiveMetastoreClient implements LuceneMetastoreClient {

	private final TTransport transport;
	private final Client client;

	public LuceneHiveMetastoreClient(TTransport tt) throws TTransportException {
		this.transport = requireNonNull(tt, "transport is null");
		this.client = new ThriftHiveMetastore.Client(
				new TBinaryProtocol(transport));
	}

	@Override
	public void close() {
		transport.close();
	}

	@Override
	public List<String> getAllDatabases() throws TException {
		return client.get_all_databases();
	}

	@Override
	public Database getDatabase(String databaseName) throws TException {
		return client.get_database(databaseName);
	}

	@Override
	public List<String> getAllTables(String databaseName) throws TException {
		return client.get_all_tables(databaseName);
	}

	@Override
	public void createTable(Table table) throws TException {
		client.create_table(table);
	}

	@Override
	public void dropTable(String databaseName, String name, boolean deleteData)
			throws TException {
		client.drop_table(databaseName, name, deleteData);
	}

	@Override
	public void alterTable(String databaseName, String tableName,
			Table newTable) throws TException {
		client.alter_table(databaseName, tableName, newTable);
	}

	@Override
	public Table getTable(String databaseName, String tableName)
			throws TException {
		return client.get_table(databaseName, tableName);
	}

	@Override
	public List<Role> listRoles(String principalName,
			PrincipalType principalType) throws TException {
		return client.list_roles(principalName, principalType);
	}

	@Override
	public PrincipalPrivilegeSet getPrivilegeSet(HiveObjectRef hiveObject,
			String userName, List<String> groupNames) throws TException {
		return client.get_privilege_set(hiveObject, userName, groupNames);
	}
}
