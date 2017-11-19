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
package com.facebook.presto.atop;

import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.reflect.Reflection.newProxy;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestAtopPlugin
{
    @Test
    public void testGetConnectorFactory()
            throws Exception
    {
        AtopPlugin plugin = new AtopPlugin();
        plugin.setTypeManager(new TypeRegistry());
        plugin.setNodeManager(newProxy(NodeManager.class, (proxy, method, args) -> {
            throw new UnsupportedOperationException();
        }));
        plugin.setOptionalConfig(ImmutableMap.<String, String>builder()
                .put("node.environment", "test")
                .build());

        assertInstanceOf(getOnlyElement(plugin.getServices(ConnectorFactory.class)), AtopConnectorFactory.class);
    }
}
