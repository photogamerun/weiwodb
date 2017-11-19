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
package com.facebook.presto.raptor.metadata;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@Test(singleThreaded = true)
public class TestMetadataDao
{
    private MetadataDao dao;
    private Handle dummyHandle;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        dao = dbi.onDemand(MetadataDao.class);
        createTablesWithRetry(dbi);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        dummyHandle.close();
    }

    @Test
    public void testTemporalColumn()
            throws Exception
    {
        Long columnId = 1L;
        long tableId = dao.insertTable("schema1", "table1", true, null);
        dao.insertColumn(tableId, columnId, "col1", 1, "bigint", null, null);
        Long temporalColumnId = dao.getTemporalColumnId(tableId);
        assertNull(temporalColumnId);

        dao.updateTemporalColumnId(tableId, columnId);
        temporalColumnId = dao.getTemporalColumnId(tableId);
        assertNotNull(temporalColumnId);
        assertEquals(temporalColumnId, columnId);

        long tableId2 = dao.insertTable("schema1", "table2", true, null);
        Long columnId2 = dao.getTemporalColumnId(tableId2);
        assertNull(columnId2);
    }
}
