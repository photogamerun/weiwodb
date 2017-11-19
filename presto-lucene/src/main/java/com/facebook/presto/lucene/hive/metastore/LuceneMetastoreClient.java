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

import java.io.Closeable;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

public interface LuceneMetastoreClient extends Closeable {

	@Override
	void close();

	List<String> getAllDatabases() throws TException;

	Database getDatabase(String databaseName) throws TException;

	List<String> getAllTables(String databaseName) throws TException;

	void createTable(Table table) throws TException;

	void dropTable(String databaseName, String name, boolean deleteData)
			throws TException;

	void alterTable(String databaseName, String tableName, Table newTable)
			throws TException;

	Table getTable(String databaseName, String tableName) throws TException;

	public List<Role> listRoles(String principalName,
			PrincipalType principalType) throws TException;

	public PrincipalPrivilegeSet getPrivilegeSet(HiveObjectRef hiveObject,
			String userName, List<String> groupNames) throws TException;

}
