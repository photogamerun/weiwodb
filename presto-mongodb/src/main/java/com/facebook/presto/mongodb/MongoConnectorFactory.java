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
package com.facebook.presto.mongodb;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class MongoConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Map<String, String> optionalConfig;
    private final TypeManager typeManager;

    public MongoConnectorFactory(String name, TypeManager typeManager, Map<String, String> optionalConfig)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.optionalConfig = requireNonNull(optionalConfig, "optionalConfig is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new MongoHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config)
    {
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new MongoClientModule(),
                    binder -> {
                        binder.bind(TypeManager.class).toInstance(typeManager);
                        binder.bind(MongoConnectorId.class).toInstance(new MongoConnectorId(connectorId));
                    });

            Injector injector = app.strictConfig().doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(MongoConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
