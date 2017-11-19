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

import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.TypeManager;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import static com.facebook.presto.hive.util.Types.checkType;
import static java.util.Objects.requireNonNull;

public class HivePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final HiveMetastore metastore;
    private final PageIndexerFactory pageIndexerFactory;
    private final TypeManager typeManager;
    private final int maxOpenPartitions;
    private final boolean immutablePartitions;
    private final boolean compressed;
    private final LocationService locationService;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;

    @Inject
    public HivePageSinkProvider(
            HdfsEnvironment hdfsEnvironment,
            HiveMetastore metastore,
            PageIndexerFactory pageIndexerFactory,
            TypeManager typeManager,
            HiveClientConfig config,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.maxOpenPartitions = config.getMaxPartitionsPerWriter();
        this.immutablePartitions = config.isImmutablePartitions();
        this.compressed = config.getHiveCompressionCodec() != HiveCompressionCodec.NONE;
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        HiveWritableTableHandle handle = checkType(tableHandle, HiveOutputTableHandle.class, "tableHandle");
        return createPageSink(handle, true, session);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorInsertTableHandle tableHandle)
    {
        HiveInsertTableHandle handle = checkType(tableHandle, HiveInsertTableHandle.class, "tableHandle");
        return createPageSink(handle, false, session);
    }

    private ConnectorPageSink createPageSink(HiveWritableTableHandle handle, boolean isCreateTable, ConnectorSession session)
    {
        return new HivePageSink(
                handle.getSchemaName(),
                handle.getTableName(),
                isCreateTable,
                handle.getInputColumns(),
                handle.getTableStorageFormat(),
                handle.getPartitionStorageFormat(),
                handle.getLocationHandle(),
                locationService,
                handle.getFilePrefix(),
                handle.getBucketProperty(),
                metastore,
                pageIndexerFactory,
                typeManager,
                hdfsEnvironment,
                maxOpenPartitions,
                immutablePartitions,
                compressed,
                partitionUpdateCodec,
                session);
    }
}
