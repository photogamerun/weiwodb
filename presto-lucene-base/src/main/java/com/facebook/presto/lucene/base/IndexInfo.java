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
package com.facebook.presto.lucene.base;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IndexInfo {

    private final String schema;
    private final String table;
    private final String partition;
    private final String id;
    private final String indexName;

    @JsonCreator
    public IndexInfo(@JsonProperty("schema") String schema, @JsonProperty("table") String table,
            @JsonProperty("partition") String partition, @JsonProperty("id") String id,
            @JsonProperty("indexName") String indexName) {
        this.schema = requireNonNull(schema, "db is null");
        this.table = requireNonNull(table, "table is null");
        this.partition = requireNonNull(partition, "partition is null");
        this.id = requireNonNull(id, "id is null");
        this.indexName = requireNonNull(indexName, "db is null");
    }

    @JsonProperty
    public String getSchema() {
        return schema;
    }

    @JsonProperty
    public String getTable() {
        return table;
    }

    @JsonProperty
    public String getPartition() {
        return partition;
    }

    @JsonProperty
    public String getId() {
        return id;
    }

    @JsonProperty
    public String getIndexName() {
        return indexName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, table, partition, id, indexName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final IndexInfo other = (IndexInfo) obj;
        if (other.getSchema() == null || other.getTable() == null || other.getPartition() == null || other.getId() == null
                || other.getIndexName() == null) {
            return false;
        }
        return (other.getSchema().equals(this.getSchema()) && other.getTable().equals(this.getTable())
                && other.getPartition().equals(this.getPartition()) && other.getId().equals(this.getId())
                && other.getIndexName().equals(this.getIndexName()));
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("schema=");
        sb.append(schema);
        sb.append(",table=");
        sb.append(table);
        sb.append(",partition=");
        sb.append(partition);
        sb.append(",id=");
        sb.append(id);
        sb.append(",indexName=");
        sb.append(indexName);
        return sb.toString();
    }

}
