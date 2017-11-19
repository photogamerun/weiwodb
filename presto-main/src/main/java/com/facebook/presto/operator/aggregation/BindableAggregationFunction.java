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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.metadata.SignatureBinder.bindVariables;
import static com.facebook.presto.operator.aggregation.AggregationCompiler.isParameterBlock;
import static com.facebook.presto.operator.aggregation.AggregationCompiler.isParameterNullable;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.SAMPLE_WEIGHT;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.fromSqlType;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class BindableAggregationFunction
    extends SqlAggregationFunction
{
    private final String description;
    private final boolean approximate;
    private final boolean decomposable;

    private final Class<?> definitionClass;
    private final Class<?> stateClass;
    private final Method inputFunction;
    private final Method outputFunction;

    public BindableAggregationFunction(Signature signature,
            String description,
            boolean approximate,
            boolean decomposable,
            Class<?> definitionClass,
            Class<?> stateClass,
            Method inputFunction,
            Method outputFunction)
    {
        super(signature);
        this.description = description;
        this.approximate = approximate;
        this.decomposable = decomposable;
        this.definitionClass = definitionClass;
        this.stateClass = stateClass;
        this.inputFunction = inputFunction;
        this.outputFunction = outputFunction;
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables variables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        // bind variables
        Signature boundSignature = bindVariables(getSignature(), variables, arity);
        List<Type> inputTypes = boundSignature.getArgumentTypes().stream().map(x -> typeManager.getType(x)).collect(toImmutableList());
        Type outputType = typeManager.getType(boundSignature.getReturnType());

        AggregationFunction aggregationAnnotation = definitionClass.getAnnotation(AggregationFunction.class);
        requireNonNull(aggregationAnnotation, "aggregationAnnotation is null");

        DynamicClassLoader classLoader = new DynamicClassLoader(definitionClass.getClassLoader());

        AggregationMetadata metadata;
        AccumulatorStateSerializer<?> stateSerializer = new StateCompiler().generateStateSerializer(stateClass, classLoader);
        Type intermediateType = stateSerializer.getSerializedType();
        Method intermediateInputFunction = AggregationCompiler.getIntermediateInputFunction(definitionClass, stateClass);
        Method combineFunction = AggregationCompiler.getCombineFunction(definitionClass, stateClass);
        AccumulatorStateFactory<?> stateFactory = new StateCompiler().generateStateFactory(stateClass, classLoader);

        try {
            MethodHandle inputHandle = lookup().unreflect(inputFunction);
            MethodHandle intermediateInputHandle = intermediateInputFunction == null ? null : lookup().unreflect(intermediateInputFunction);
            MethodHandle combineHandle = combineFunction == null ? null : lookup().unreflect(combineFunction);
            MethodHandle outputHandle = outputFunction == null ? null : lookup().unreflect(outputFunction);
            metadata = new AggregationMetadata(
                    generateAggregationName(getSignature().getName(), outputType.getTypeSignature(), signaturesFromTypes(inputTypes)),
                    getParameterMetadata(inputFunction, inputTypes),
                    inputHandle,
                    getParameterMetadata(intermediateInputFunction, inputTypes),
                    intermediateInputHandle,
                    combineHandle,
                    outputHandle,
                    stateClass,
                    stateSerializer,
                    stateFactory,
                    outputType,
                    aggregationAnnotation.approximate());
        }
        catch (IllegalAccessException e) {
            throw Throwables.propagate(e);
        }

        AccumulatorFactoryBinder factory = new LazyAccumulatorFactoryBinder(metadata, classLoader);

        return new InternalAggregationFunction(getSignature().getName(),
                inputTypes,
                intermediateType,
                outputType,
                decomposable,
                approximate,
                factory);
    }

    public InternalAggregationFunction specialize(BoundVariables variables, int arity, TypeManager typeManager)
    {
        return specialize(variables, arity, typeManager, null);
    }

    private static List<TypeSignature> signaturesFromTypes(List<Type> types)
    {
        return types
                .stream()
                .map(x -> x.getTypeSignature())
                .collect(toImmutableList());
    }

    private static List<ParameterMetadata> getParameterMetadata(@Nullable Method method, List<Type> inputTypes)
    {
        if (method == null) {
            return null;
        }

        ImmutableList.Builder<ParameterMetadata> builder = ImmutableList.builder();
        builder.add(new ParameterMetadata(STATE));

        Annotation[][] annotations = method.getParameterAnnotations();
        String methodName = method.getDeclaringClass() + "." + method.getName();

        // Start at 1 because 0 is the STATE
        for (int i = 1; i < annotations.length; i++) {
            Annotation baseTypeAnnotation = baseTypeAnnotation(annotations[i], methodName);
            if (baseTypeAnnotation instanceof SqlType) {
                builder.add(fromSqlType(inputTypes.get(i - 1), isParameterBlock(annotations[i]), isParameterNullable(annotations[i]), methodName));
            }
            else if (baseTypeAnnotation instanceof BlockIndex) {
                builder.add(new ParameterMetadata(BLOCK_INDEX));
            }
            else if (baseTypeAnnotation instanceof SampleWeight) {
                builder.add(new ParameterMetadata(SAMPLE_WEIGHT));
            }
            else {
                throw new IllegalArgumentException("Unsupported annotation: " + annotations[i]);
            }
        }
        return builder.build();
    }

    private static Annotation baseTypeAnnotation(Annotation[] annotations, String methodName)
    {
        List<Annotation> baseTypes = Arrays.asList(annotations).stream()
                .filter(annotation -> annotation instanceof SqlType || annotation instanceof BlockIndex || annotation instanceof SampleWeight)
                .collect(toImmutableList());

        checkArgument(baseTypes.size() == 1, "Parameter of %s must have exactly one of @SqlType, @BlockIndex, and @SampleWeight", methodName);

        boolean nullable = isParameterNullable(annotations);
        boolean isBlock = isParameterBlock(annotations);

        Annotation annotation = baseTypes.get(0);
        checkArgument((!isBlock && !nullable) || (annotation instanceof SqlType),
                "%s contains a parameter with @BlockPosition and/or @NullablePosition that is not @SqlType", methodName);

        return annotation;
    }
}
