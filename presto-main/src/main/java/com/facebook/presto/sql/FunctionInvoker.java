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
package com.facebook.presto.sql;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.base.Throwables;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FunctionInvoker
{
    private final FunctionRegistry registry;

    public FunctionInvoker(FunctionRegistry registry)
    {
        this.registry = requireNonNull(registry, "registry is null");
    }

    public Object invoke(Signature function, ConnectorSession session, Object... arguments)
    {
        return invoke(function, session, Arrays.asList(arguments));
    }

    /**
     * Arguments must be the native container type for the corresponding SQL types.
     *
     * Returns a value in the native container type corresponding to the declared SQL return type
     */
    public Object invoke(Signature function, ConnectorSession session, List<Object> arguments)
    {
        MethodHandle method = registry.getScalarFunctionImplementation(function).getMethodHandle();

        List<Object> actualArguments = new ArrayList<>(arguments.size() + 1);

        Iterator<Object> iterator = arguments.iterator();
        for (int i = 0; i < method.type().parameterCount(); i++) {
            if (method.type().parameterType(i) == ConnectorSession.class) {
                actualArguments.add(session);
            }
            else {
                checkArgument(iterator.hasNext(), "Not enough arguments provided for method: %s", method.type());
                actualArguments.add(iterator.next());
            }
        }

        checkArgument(!iterator.hasNext(), "Too many arguments provided for method: %s", method.type());

        try {
            return method.invokeWithArguments(actualArguments);
        }
        catch (Throwable throwable) {
            throw Throwables.propagate(throwable);
        }
    }
}
