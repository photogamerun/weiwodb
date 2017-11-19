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

import static java.util.Objects.requireNonNull;

import java.util.List;

import javax.annotation.Nullable;

import com.facebook.presto.lucene.base.IndexInfo;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.pushdown.PushDown;
import com.facebook.presto.sql.pair.SimpleFilterPair;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

public class LuceneSplit implements ConnectorSplit {

	private final String connectorId;
	private final String catalogName;
	private final List<IndexInfo> indexs;
	private final List<HostAddress> addresses;
	private final TupleDomain<ColumnHandle> tupleDomain;
	private final Boolean isWriter;
	private PushDown pushDowns;

	@JsonCreator
	public LuceneSplit(@JsonProperty("connectorId") String connectorId,
			@JsonProperty("catalogName") @Nullable String catalogName,
			@JsonProperty("indexs") @Nullable List<IndexInfo> indexs,
			@JsonProperty("addresses") List<HostAddress> addresses,
			@JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
			@JsonProperty("isWriter") Boolean isWriter) {
		this.connectorId = requireNonNull(connectorId, "connectorId is null");
		this.catalogName = requireNonNull(catalogName, "catalogName is null");
		this.indexs = requireNonNull(indexs, "indexs is null");
		this.addresses = requireNonNull(addresses, "addresses is null");
		this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
		this.isWriter = requireNonNull(isWriter, "isWriter is null");
	}

	@Override
	public boolean isRemotelyAccessible() {
		if (addresses == null || addresses.size() < 1) {
			return true;
		} else {
			return false;
		}
	}

	@JsonProperty
	@Override
	public List<HostAddress> getAddresses() {
		return addresses;
	}

	@Override
	public Object getInfo() {
		return ImmutableMap.builder().put("connectorId", connectorId)
				.put("catalogName", catalogName).put("indexs", indexs)
				.put("addresses", addresses).put("tupleDomain", tupleDomain)
				.build();
	}

	@JsonProperty
	public String getConnectorId() {
		return connectorId;
	}

	@JsonProperty
	public String getCatalogName() {
		return catalogName;
	}

	@JsonProperty
	public List<IndexInfo> getIndexs() {
		return indexs;
	}

	@JsonProperty
	public TupleDomain<ColumnHandle> getTupleDomain() {
		return tupleDomain;
	}

	@JsonProperty
	public Boolean getIsWriter() {
		return isWriter;
	}

	@Override
	public PushDown getPushDown() {
		if (pushDowns == null) {
			SimpleFilterPair filter = new SimpleFilterPair(
					BooleanLiteral.TRUE_LITERAL);
			this.pushDowns = new PushDown(filter, null, null, null);
		}
		return pushDowns;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(getInfo());
		return sb.toString();
	}
}