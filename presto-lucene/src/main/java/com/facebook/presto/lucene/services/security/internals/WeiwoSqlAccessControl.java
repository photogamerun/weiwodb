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
package com.facebook.presto.lucene.services.security.internals;

import static com.facebook.presto.lucene.services.security.internals.LucenePrivilegeInfo.LucenePrivilege.ALTER;
import static com.facebook.presto.lucene.services.security.internals.LucenePrivilegeInfo.LucenePrivilege.CREATE;
import static com.facebook.presto.lucene.services.security.internals.LucenePrivilegeInfo.LucenePrivilege.DROP;
import static com.facebook.presto.lucene.services.security.internals.LucenePrivilegeInfo.LucenePrivilege.SELECT;
import static com.facebook.presto.lucene.services.security.internals.LucenePrivilegeInfo.LucenePrivilege.UPDATE;
import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectView;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static java.util.Objects.requireNonNull;

import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.facebook.presto.lucene.hive.metastore.LuceneMetastore;
import com.facebook.presto.lucene.services.security.SecurityConfig;
import com.facebook.presto.lucene.services.security.internals.LucenePrivilegeInfo.LucenePrivilege;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.google.common.collect.ImmutableSet;

import io.airlift.log.Logger;
/**
 * @author peter.wei
 */
public class WeiwoSqlAccessControl implements ConnectorAccessControl {
	private static final Logger log = Logger.get(WeiwoSqlAccessControl.class);
	private static final String ADMIN_ROLE_NAME = "admin";
	private static final String INFORMATION_SCHEMA_NAME = "information_schema";
	private final LuceneMetastore metastore;
	private final boolean allowDropTable;
	private final boolean allowRenameTable;

	@Inject
	public WeiwoSqlAccessControl(LuceneMetastore metastore,
			SecurityConfig weiwoConfig) {
		this.metastore = requireNonNull(metastore, "metastore is null");
		requireNonNull(weiwoConfig, "hiveClientConfig is null");
		allowDropTable = weiwoConfig.isAllowDropTable();
		allowRenameTable = weiwoConfig.isAllowRenameTable();
	}

	@Override
	public void checkCanCreateTable(ConnectorTransactionHandle transaction,
			Identity identity, SchemaTableName tableName) {
		log.debug(" ----   checkCanCreateTable " + tableName);
		if (!checkDatabasePermission(identity, tableName.getSchemaName(),
				CREATE)) {
			denyCreateTable(tableName.toString());
		}
	}

	@Override
	public void checkCanDropTable(ConnectorTransactionHandle transaction,
			Identity identity, SchemaTableName tableName) {
		log.debug(" ----   checkCanDropTable " + tableName);
		if (!allowDropTable
				|| !checkTableDBPermission(identity, tableName, DROP)) {
			denyDropTable(tableName.toString());
		}
	}

	@Override
	public void checkCanRenameTable(ConnectorTransactionHandle transaction,
			Identity identity, SchemaTableName tableName,
			SchemaTableName newTableName) {
		log.debug(" ----   checkCanRenameTable " + tableName);
		if (!allowRenameTable
				|| !checkTableDBPermission(identity, tableName, ALTER)) {
			// HiveOperation.ALTERTABLE_RENAME
			denyRenameTable(tableName.toString(), newTableName.toString());
		}
	}

	@Override
	public void checkCanSelectFromTable(ConnectorTransactionHandle transaction,
			Identity identity, SchemaTableName tableName) {
		log.debug(" ----   checkCanSelectFromTable " + tableName);
		if (!checkTableDBPermission(identity, tableName, SELECT)) {
			denySelectTable(tableName.toString());
		}
	}

	@Override
	public void checkCanInsertIntoTable(ConnectorTransactionHandle transaction,
			Identity identity, SchemaTableName tableName) {
		log.debug(" ----   checkCanInsertIntoTable " + tableName);
		// after production testing, update+create can insert
		if (!checkTableDBPermission(identity, tableName, UPDATE)) {
			denyInsertTable(tableName.toString());
		}
	}

	@Override
	public void checkCanDeleteFromTable(ConnectorTransactionHandle transaction,
			Identity identity, SchemaTableName tableName) {
		log.debug(" ----   checkCanDeleteFromTable " + tableName);
		// because hive default authorization doesn't support this, so we decide
		// delete allowed if privilege is all
		if (!checkTableDBPermission(identity, tableName,
				LucenePrivilege.values())) {
			denyDeleteTable(tableName.toString());
		}
	}

	@Override
	public void checkCanCreateView(ConnectorTransactionHandle transaction,
			Identity identity, SchemaTableName viewName) {
		log.debug(" ----   checkCanCreateView " + viewName);
		if (!checkDatabasePermission(identity, viewName.getSchemaName(),
				CREATE)) {
			// CREATEVIEW("CREATEVIEW", new Privilege[]{Privilege.SELECT}, new
			// Privilege[]{Privilege.CREATE}),
			// should test if select check is ok here.
			denyCreateView(viewName.toString());
		}
	}

	@Override
	public void checkCanDropView(ConnectorTransactionHandle transaction,
			Identity identity, SchemaTableName viewName) {
		log.debug(" ----   checkCanDropView " + viewName);
		if (!checkTableDBPermission(identity, viewName, DROP)) {
			// DROPVIEW("DROPVIEW", null, new Privilege[]{Privilege.DROP}),
			denyDropView(viewName.toString());
		}
	}

	@Override
	public void checkCanSelectFromView(ConnectorTransactionHandle transaction,
			Identity identity, SchemaTableName viewName) {
		log.debug(" ----   checkCanSelectFromView " + viewName);
		if (!checkTableDBPermission(identity, viewName, SELECT)) {
			denySelectView(viewName.toString());
		}
	}

	@Override
	public void checkCanGrantTablePrivilege(Identity identity,
			Privilege privilege, SchemaTableName tableName) {
		log.debug(" ----   checkCanGrantTablePrivilege " + tableName);
		// not allowed in vip. pls ask hive admin to grant privilege
		denyGrantTablePrivilege(privilege.name(), tableName.toString());
	}

	@Override
	public void checkCanAddColumn(ConnectorTransactionHandle transaction,
			Identity identity, SchemaTableName tableName) {
		log.debug(" ----   checkCanAddColumn " + tableName);
		if (!checkTableDBPermission(identity, tableName, ALTER)) {
			denyAddColumn(tableName.toString());
		}
	}

	@Override
	public void checkCanRenameColumn(ConnectorTransactionHandle transaction,
			Identity identity, SchemaTableName tableName) {
		log.debug(" ----   checkCanRenameColumn " + tableName);
		if (!checkTableDBPermission(identity, tableName, ALTER)) {
			denyRenameColumn(tableName.toString());
		}
	}

	@Override
	public void checkCanCreateViewWithSelectFromTable(
			ConnectorTransactionHandle transaction, Identity identity,
			SchemaTableName tableName) {
		log.debug(" ----   checkCanCreateViewWithSelectFromTable " + tableName);
		if (!checkTableDBPermission(identity, tableName, SELECT)) {
			denySelectTable(tableName.toString());
		}
	}

	@Override
	public void checkCanCreateViewWithSelectFromView(
			ConnectorTransactionHandle transaction, Identity identity,
			SchemaTableName viewName) {
		log.debug(" ----   checkCanCreateViewWithSelectFromView " + viewName);
		if (!checkTableDBPermission(identity, viewName, SELECT)) {
			denySelectView(viewName.toString());
		}
	}

	private boolean checkDatabasePermission(Identity identity,
			String schemaName, LucenePrivilege... requiredPrivileges) {
		Set<LucenePrivilege> privilegeSet = metastore
				.getDatabasePrivileges(identity.getUser(), schemaName).stream()
				.map(LucenePrivilegeInfo::getLucenePrivilege)
				.collect(Collectors.toSet());
		return privilegeSet
				.containsAll(ImmutableSet.copyOf(requiredPrivileges));
	}

	private boolean checkTableDBPermission(Identity identity,
			SchemaTableName tableName, LucenePrivilege... requiredPrivileges) {
		if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
			return true;
		}

		Set<LucenePrivilege> tablePrivilegeSet = metastore
				.getTablePrivileges(identity.getUser(),
						tableName.getSchemaName(), tableName.getTableName())
				.stream().map(LucenePrivilegeInfo::getLucenePrivilege)
				.collect(Collectors.toSet());;

		ImmutableSet.Builder<LucenePrivilege> privileges = ImmutableSet
				.builder();
		ImmutableSet<LucenePrivilege> privilegesSet = privileges
				.addAll(tablePrivilegeSet).build();

		return privilegesSet
				.containsAll(ImmutableSet.copyOf(requiredPrivileges));
	}

	@Override
	public void checkCanSetCatalogSessionProperty(Identity identity,
			String propertyName) {
		if (!metastore.getRoles(identity.getUser()).contains(ADMIN_ROLE_NAME)) {
			denySetCatalogSessionProperty("weiwodb", propertyName);
		}
	}

	@Override
	public void checkCanRevokeTablePrivilege(Identity identity,
			Privilege privilege, SchemaTableName tableName) {
		if (checkTablePermission(identity, tableName, CREATE)) {
			return;
		}

		LucenePrivilege hivePrivilege = LucenePrivilegeInfo
				.toLucenePrivilege(privilege);
		if (hivePrivilege == null || !getGrantOptionForPrivilege(identity,
				privilege, tableName)) {
			denyRevokeTablePrivilege(privilege.name(), tableName.toString());
		}
	}

	private boolean getGrantOptionForPrivilege(Identity identity,
			Privilege privilege, SchemaTableName tableName) {
		return metastore
				.getTablePrivileges(identity.getUser(),
						tableName.getSchemaName(), tableName.getTableName())
				.contains(new LucenePrivilegeInfo(
						LucenePrivilegeInfo.toLucenePrivilege(privilege),
						true));
	}

	private boolean checkTablePermission(Identity identity,
			SchemaTableName tableName, LucenePrivilege... requiredPrivileges) {
		if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
			return true;
		}

		Set<LucenePrivilege> privilegeSet = metastore
				.getTablePrivileges(identity.getUser(),
						tableName.getSchemaName(), tableName.getTableName())
				.stream().map(LucenePrivilegeInfo::getLucenePrivilege)
				.collect(Collectors.toSet());
		return privilegeSet
				.containsAll(ImmutableSet.copyOf(requiredPrivileges));
	}
}