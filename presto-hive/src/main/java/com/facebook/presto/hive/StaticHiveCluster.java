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

import com.facebook.presto.hive.metastore.HiveMetastoreClient;
import com.facebook.presto.spi.PrestoException;
import com.google.common.net.HostAndPort;
import org.apache.thrift.transport.TTransportException;

import javax.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class StaticHiveCluster
        implements HiveCluster
{
    private final List<HostAndPort> addresses;
    private final HiveMetastoreClientFactory clientFactory;

    @Inject
    public StaticHiveCluster(StaticMetastoreConfig config, HiveMetastoreClientFactory clientFactory)
    {
        this(config.getMetastoreUris(), clientFactory);
    }

    public StaticHiveCluster(List<URI> metastoreUris, HiveMetastoreClientFactory clientFactory)
    {
        requireNonNull(metastoreUris, "metastoreUris is null");
        checkArgument(!metastoreUris.isEmpty(), "metastoreUris must specify at least one URI");
        this.addresses = metastoreUris.stream()
                .map(StaticHiveCluster::checkMetastoreUri)
                .map(uri -> HostAndPort.fromParts(uri.getHost(), uri.getPort()))
                .collect(toList());
        this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
    }

    /**
     * Create a metastore client connected to the Hive metastore.
     * <p>
     * As per Hive HA metastore behavior, return the first metastore in the list
     * list of available metastores (i.e. the default metastore) if a connection
     * can be made, else try another of the metastores at random, until either a
     * connection succeeds or there are no more fallback metastores.
     */
    @Override
    public HiveMetastoreClient createMetastoreClient()
    {
        List<HostAndPort> metastores = new ArrayList<>(addresses);
        Collections.shuffle(metastores.subList(1, metastores.size()));

        TTransportException lastException = null;
        for (HostAndPort metastore : metastores) {
            try {
                return clientFactory.create(metastore.getHostText(), metastore.getPort());
            }
            catch (TTransportException e) {
                lastException = e;
            }
        }

        throw new PrestoException(HIVE_METASTORE_ERROR, "Failed connecting to Hive metastore: " + addresses, lastException);
    }

    private static URI checkMetastoreUri(URI uri)
    {
        requireNonNull(uri, "metastoreUri is null");
        String scheme = uri.getScheme();
        checkArgument(!isNullOrEmpty(scheme), "metastoreUri scheme is missing: %s", uri);
        checkArgument(scheme.equals("thrift"), "metastoreUri scheme must be thrift: %s", uri);
        checkArgument(uri.getHost() != null, "metastoreUri host is missing: %s", uri);
        checkArgument(uri.getPort() != -1, "metastoreUri port is missing: %s", uri);
        return uri;
    }
}
