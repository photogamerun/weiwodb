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

import java.util.Objects;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LuceneTableLayoutHandle implements ConnectorTableLayoutHandle{
    
    private final LuceneTableHandle table;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public LuceneTableLayoutHandle(
            @JsonProperty("table") LuceneTableHandle table,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> domain)
    {
        this.table = requireNonNull(table, "table is null");
        this.tupleDomain = requireNonNull(domain, "tupleDomain is null");
    }

    @JsonProperty
    public LuceneTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LuceneTableLayoutHandle that = (LuceneTableLayoutHandle) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(tupleDomain, that.tupleDomain);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, tupleDomain);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }

}
