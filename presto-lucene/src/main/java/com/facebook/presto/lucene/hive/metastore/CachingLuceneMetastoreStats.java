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

import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class CachingLuceneMetastoreStats {

	private final LuceneMetastoreApiStats getAllDatabases = new LuceneMetastoreApiStats();
	private final LuceneMetastoreApiStats getDatabase = new LuceneMetastoreApiStats();
	private final LuceneMetastoreApiStats getAllTables = new LuceneMetastoreApiStats();
	private final LuceneMetastoreApiStats getTable = new LuceneMetastoreApiStats();
	private final LuceneMetastoreApiStats createTable = new LuceneMetastoreApiStats();
	private final LuceneMetastoreApiStats dropTable = new LuceneMetastoreApiStats();
	private final LuceneMetastoreApiStats alterTable = new LuceneMetastoreApiStats();
	private final LuceneMetastoreApiStats loadRoles = new LuceneMetastoreApiStats();
	private final LuceneMetastoreApiStats getPrivilegeSet = new LuceneMetastoreApiStats();

	@Managed
	@Nested
	public LuceneMetastoreApiStats getGetAllDatabases() {
		return getAllDatabases;
	}

	@Managed
	@Nested
	public LuceneMetastoreApiStats getGetDatabase() {
		return getDatabase;
	}

	@Managed
	@Nested
	public LuceneMetastoreApiStats getGetAllTables() {
		return getAllTables;
	}

	@Managed
	@Nested
	public LuceneMetastoreApiStats getGetTable() {
		return getTable;
	}

	@Managed
	@Nested
	public LuceneMetastoreApiStats getCreateTable() {
		return createTable;
	}

	@Managed
	@Nested
	public LuceneMetastoreApiStats getDropTable() {
		return dropTable;
	}

	@Managed
	@Nested
	public LuceneMetastoreApiStats getAlterTable() {
		return alterTable;
	}

	@Managed
	@Nested
	public LuceneMetastoreApiStats getLoadRoles() {
		return loadRoles;
	}

	@Managed
	@Nested
	public LuceneMetastoreApiStats getGetPrivilegeSet() {
		return getPrivilegeSet;
	}
}
