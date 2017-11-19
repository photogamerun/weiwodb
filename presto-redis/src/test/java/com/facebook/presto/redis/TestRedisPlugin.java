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
package com.facebook.presto.redis;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test
public class TestRedisPlugin
{
    @Test
    public ConnectorFactory testConnectorExists()
    {
        RedisPlugin plugin = new RedisPlugin();
        plugin.setTypeManager(new TestingTypeManager());
        plugin.setNodeManager(new TestingNodeManager());

        List<ConnectorFactory> factories = plugin.getServices(ConnectorFactory.class);
        assertNotNull(factories);
        assertEquals(factories.size(), 1);
        ConnectorFactory factory = factories.get(0);
        assertNotNull(factory);
        return factory;
    }

    @Test
    public void testStartup()
    {
        ConnectorFactory factory = testConnectorExists();
        Connector c = factory.create("test-connector", ImmutableMap.<String, String>builder()
                .put("redis.table-names", "test")
                .put("redis.nodes", "localhost:6379")
                .build());
        assertNotNull(c);
    }

    private static class TestingTypeManager
            implements TypeManager
    {
        @Override
        public Type getType(TypeSignature signature)
        {
            return null;
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
        {
            return null;
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public Optional<Type> getCommonSuperType(List<? extends Type> types)
        {
            return Optional.empty();
        }

        @Override
        public boolean isTypeOnlyCoercion(Type actualType, Type expectedType)
        {
            return false;
        }

        @Override
        public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
        {
            return Optional.empty();
        }

        @Override
        public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestingNodeManager
            implements NodeManager
    {
        private static final Node LOCAL_NODE = new TestingNode();

        @Override
        public Set<Node> getNodes(NodeState state)
        {
            return ImmutableSet.of(LOCAL_NODE);
        }

        @Override
        public Set<Node> getActiveDatasourceNodes(String datasourceName)
        {
            return ImmutableSet.of(LOCAL_NODE);
        }

        @Override
        public Node getCurrentNode()
        {
            return LOCAL_NODE;
        }

        @Override
        public Set<Node> getCoordinators()
        {
            return ImmutableSet.of(LOCAL_NODE);
        }

        @Override
        public Set<Node> getWeiwomanagers() {
            return ImmutableSet.of(LOCAL_NODE);
        }
    }

    private static class TestingNode
            implements Node
    {
        @Override
        public HostAddress getHostAndPort()
        {
            return HostAddress.fromParts("localhost", 6379);
        }

        @Override
        public URI getHttpUri()
        {
            return URI.create("http://localhost:6379/");
        }

        @Override
        public String getNodeIdentifier()
        {
            return UUID.randomUUID().toString();
        }
    }
}
