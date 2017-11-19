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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import com.facebook.presto.lucene.hive.metastore.HiveType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LuceneColumnHandle implements ColumnHandle {

	private final String connectorId;
	private final String columnName;
	private final HiveType columnType;
	private final int hiveColumnIndex;
	private final TypeSignature typeName;
	private final String luceneType;

	@JsonCreator
	public LuceneColumnHandle(@JsonProperty("connectorId") String connectorId,
			@JsonProperty("columnName") String columnName,
			@JsonProperty("columnType") HiveType columnType,
			@JsonProperty("hiveColumnIndex") int hiveColumnIndex,
			@JsonProperty("typeSignature") TypeSignature typeSignature,
			@JsonProperty("luceneType") String luceneType) {
		this.connectorId = requireNonNull(connectorId, "connectorId is null");
		this.columnName = requireNonNull(columnName, "columnName is null");
		this.columnType = requireNonNull(columnType, "columnType is null");
		this.hiveColumnIndex = requireNonNull(hiveColumnIndex,
				"hiveColumnIndex is null");
		this.typeName = requireNonNull(typeSignature, "type is null");
		this.luceneType = requireNonNull(luceneType, "luceneType is null");
	}

	@JsonProperty
	public TypeSignature getTypeName() {
		return typeName;
	}

	@JsonProperty
	public String getLuceneType() {
		return luceneType;
	}

	@JsonProperty
	public TypeSignature getTypeSignature() {
		return typeName;
	}

	@JsonProperty
	public String getConnectorId() {
		return connectorId;
	}

	@JsonProperty
	public String getColumnName() {
		return columnName;
	}

	@JsonProperty
	public HiveType getColumnType() {
		return columnType;
	}

	public ColumnMetadata getColumnMetadata(TypeManager typeManager) {
		return new ColumnMetadata(columnName, typeManager.getType(typeName));
	}

	@JsonProperty
	public int getHiveColumnIndex() {
		return hiveColumnIndex;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if ((obj == null) || (getClass() != obj.getClass())) {
			return false;
		}
		LuceneColumnHandle o = (LuceneColumnHandle) obj;
		return Objects.equals(this.connectorId, o.connectorId)
				&& Objects.equals(this.columnName, o.columnName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(connectorId, columnName);
	}

	@Override
	public String toString() {
		return toStringHelper(this).add("connectorId", connectorId)
				.add("columnName", columnName).add("columnType", columnType)
				.add("hiveColumnIndex", hiveColumnIndex)
				.add("luceneType", luceneType).toString();
	}

	public Category getCategory() {
		return Category.FIELD;
	}

	public enum Category {
		FUNCT, FIELD, HASH
	}
}
