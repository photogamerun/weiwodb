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

import static java.util.Locale.ENGLISH;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;

import com.facebook.presto.spi.security.Privilege;
import com.google.common.collect.ImmutableSet;
/**
 * 
 * 
 * @author peter.wei
 *
 */
public class LucenePrivilegeInfo {
	public enum LucenePrivilege {
		SELECT, UPDATE, DELETE, DROP, ALTER, CREATE;
	}

	private final LucenePrivilege hivePrivilege;
	private final boolean grantOption;

	public LucenePrivilegeInfo(LucenePrivilege hivePrivilege,
			boolean grantOption) {
		this.hivePrivilege = hivePrivilege;
		this.grantOption = grantOption;
	}

	public LucenePrivilege getLucenePrivilege() {
		return hivePrivilege;
	}

	public boolean isGrantOption() {
		return grantOption;
	}

	public LucenePrivilegeInfo withGrantOption(boolean grantOption) {
		return new LucenePrivilegeInfo(hivePrivilege, grantOption);
	}

	public static Set<LucenePrivilegeInfo> parsePrivilege(
			PrivilegeGrantInfo userGrant) {
		boolean withGrantOption = userGrant.isGrantOption();
		String name = userGrant.getPrivilege().toUpperCase(ENGLISH);
		switch (name) {
			case "ALL" :
				return Arrays.asList(LucenePrivilege.values()).stream()
						.map(hivePrivilege -> new LucenePrivilegeInfo(
								hivePrivilege, withGrantOption))
						.collect(Collectors.toSet());
			case "SELECT" :
				return ImmutableSet.of(new LucenePrivilegeInfo(
						LucenePrivilege.SELECT, withGrantOption));
			case "UPDATE" :
				return ImmutableSet.of(new LucenePrivilegeInfo(
						LucenePrivilege.UPDATE, withGrantOption));
			case "DELETE" :
				return ImmutableSet.of(new LucenePrivilegeInfo(
						LucenePrivilege.DELETE, withGrantOption));
			case "DROP" :
				return ImmutableSet.of(new LucenePrivilegeInfo(
						LucenePrivilege.DROP, withGrantOption));
			case "ALTER" :
				return ImmutableSet.of(new LucenePrivilegeInfo(
						LucenePrivilege.ALTER, withGrantOption));
			case "CREATE" :
				return ImmutableSet.of(new LucenePrivilegeInfo(
						LucenePrivilege.CREATE, withGrantOption));
		}
		return ImmutableSet.of();
	}

	public static LucenePrivilege toLucenePrivilege(Privilege privilege) {
		switch (privilege) {
			case SELECT :
				return LucenePrivilege.SELECT;
			case INSERT :
				return LucenePrivilege.UPDATE;
			case DELETE :
				return LucenePrivilege.DELETE;
		}
		return null;
	}

	@Override
	public int hashCode() {
		return Objects.hash(hivePrivilege, grantOption);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LucenePrivilegeInfo hivePrivilegeInfo = (LucenePrivilegeInfo) o;
		return Objects.equals(hivePrivilege, hivePrivilegeInfo.hivePrivilege)
				&& Objects.equals(grantOption, hivePrivilegeInfo.grantOption);
	}
}
