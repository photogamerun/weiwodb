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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static java.util.Objects.requireNonNull;

public class HivePartition
{
    public static final String UNPARTITIONED_ID = "<UNPARTITIONED>";

    private final SchemaTableName tableName;
    private final TupleDomain<HiveColumnHandle> effectivePredicate;
    private final String partitionId;
    private final Map<ColumnHandle, NullableValue> keys;
    private final Optional<HiveBucket> bucket;

    public HivePartition(SchemaTableName tableName, TupleDomain<HiveColumnHandle> effectivePredicate)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.partitionId = UNPARTITIONED_ID;
        this.keys = ImmutableMap.of();
        this.bucket = Optional.empty();
    }

    public HivePartition(SchemaTableName tableName, TupleDomain<HiveColumnHandle> effectivePredicate, Optional<HiveBucket> bucket)
    {
        this(tableName, effectivePredicate, UNPARTITIONED_ID, ImmutableMap.of(), bucket);
    }

    public HivePartition(SchemaTableName tableName,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            String partitionId,
            Map<ColumnHandle, NullableValue> keys,
            Optional<HiveBucket> bucket)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.partitionId = requireNonNull(partitionId, "partitionId is null");
        this.keys = ImmutableMap.copyOf(requireNonNull(keys, "keys is null"));
        this.bucket = requireNonNull(bucket, "bucket number is null");
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    public TupleDomain<HiveColumnHandle> getEffectivePredicate()
    {
        return effectivePredicate;
    }

    public String getPartitionId()
    {
        return partitionId;
    }

    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return TupleDomain.fromFixedValues(keys);
    }

    public Map<ColumnHandle, NullableValue> getKeys()
    {
        return keys;
    }

    public Optional<HiveBucket> getBucket()
    {
        return bucket;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        HivePartition other = (HivePartition) obj;
        return Objects.equals(this.partitionId, other.partitionId);
    }

    @Override
    public String toString()
    {
        return tableName + ":" + partitionId;
    }
}
