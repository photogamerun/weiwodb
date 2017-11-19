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
package com.facebook.presto.cassandra;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

import static com.facebook.presto.cassandra.CassandraQueryRunner.createCassandraQueryRunner;
import static com.facebook.presto.cassandra.CassandraQueryRunner.createSampledSession;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static org.testng.Assert.assertEquals;

//Integrations tests fail when parallel, due to a bug or configuration error in the embedded
//cassandra instance. This problem results in either a hang in Thrift calls or broken sockets.
@Test(singleThreaded = true)
public class TestCassandraDistributed
        extends AbstractTestDistributedQueries
{
    public TestCassandraDistributed()
            throws Exception
    {
        super(createCassandraQueryRunner(TpchTable.getTables()), createSampledSession());
    }

    @Override
    public void testGroupingSetMixedExpressionAndColumn()
            throws Exception
    {
        // Cassandra does not support DATE
    }

    @Override
    public void testGroupingSetMixedExpressionAndOrdinal()
            throws Exception
    {
        // Cassandra does not support DATE
    }

    @Override
    public void testRenameTable()
            throws Exception
    {
        // Cassandra does not support renaming tables
    }

    @Override
    public void testAddColumn()
            throws Exception
    {
        // Cassandra does not support adding columns
    }

    @Override
    public void testRenameColumn()
            throws Exception
    {
        // Cassandra does not support renaming columns
    }

    @Override
    public void testView()
            throws Exception
    {
        // Cassandra connector currently does not support views
    }

    @Override
    public void testCompatibleTypeChangeForView()
            throws Exception
    {
        // Cassandra connector currently does not support views
    }

    @Override
    public void testCompatibleTypeChangeForView2()
            throws Exception
    {
        // Cassandra connector currently does not support views
    }

    @Override
    public void testViewMetadata()
            throws Exception
    {
        // Cassandra connector currently does not support views
    }

    @Override
    public void testInsert()
            throws Exception
    {
        // Cassandra connector currently does not support insert
    }

    @Override
    public void testCreateTable()
            throws Exception
    {
        // Cassandra connector currently does not support create table
    }

    @Override
    public void testCreateTableAsSelect()
            throws Exception
    {
        // Cassandra connector currently does not support create table
    }

    @Override
    public void testDelete()
            throws Exception
    {
        // Cassandra connector currently does not support delete
    }

    public void testShowColumns()
            throws Exception
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "")
                .row("custkey", "bigint", "")
                .row("orderstatus", "varchar", "")
                .row("totalprice", "double", "")
                .row("orderdate", "varchar", "")
                .row("orderpriority", "varchar", "")
                .row("clerk", "varchar", "")
                .row("shippriority", "integer", "")
                .row("comment", "varchar", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }
}
