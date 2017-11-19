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
package com.facebook.presto.metadata;

import com.facebook.presto.block.BlockSerdeUtil;
import com.facebook.presto.operator.aggregation.ApproximateAverageAggregations;
import com.facebook.presto.operator.aggregation.ApproximateCountAggregation;
import com.facebook.presto.operator.aggregation.ApproximateCountColumnAggregations;
import com.facebook.presto.operator.aggregation.ApproximateCountDistinctAggregations;
import com.facebook.presto.operator.aggregation.ApproximateDoublePercentileAggregations;
import com.facebook.presto.operator.aggregation.ApproximateDoublePercentileArrayAggregations;
import com.facebook.presto.operator.aggregation.ApproximateFloatPercentileAggregations;
import com.facebook.presto.operator.aggregation.ApproximateFloatPercentileArrayAggregations;
import com.facebook.presto.operator.aggregation.ApproximateLongPercentileAggregations;
import com.facebook.presto.operator.aggregation.ApproximateLongPercentileArrayAggregations;
import com.facebook.presto.operator.aggregation.ApproximateSetAggregation;
import com.facebook.presto.operator.aggregation.ApproximateSumAggregations;
import com.facebook.presto.operator.aggregation.ArrayAggregationFunction;
import com.facebook.presto.operator.aggregation.AverageAggregations;
import com.facebook.presto.operator.aggregation.BooleanAndAggregation;
import com.facebook.presto.operator.aggregation.BooleanOrAggregation;
import com.facebook.presto.operator.aggregation.CountAggregation;
import com.facebook.presto.operator.aggregation.CountIfAggregation;
import com.facebook.presto.operator.aggregation.DoubleCorrelationAggregation;
import com.facebook.presto.operator.aggregation.DoubleCovarianceAggregation;
import com.facebook.presto.operator.aggregation.DoubleHistogramAggregation;
import com.facebook.presto.operator.aggregation.DoubleRegressionAggregation;
import com.facebook.presto.operator.aggregation.DoubleSumAggregation;
import com.facebook.presto.operator.aggregation.FloatAverageAggregation;
import com.facebook.presto.operator.aggregation.FloatCorrelationAggregation;
import com.facebook.presto.operator.aggregation.FloatCovarianceAggregation;
import com.facebook.presto.operator.aggregation.FloatGeometricMeanAggregations;
import com.facebook.presto.operator.aggregation.FloatHistogramAggregation;
import com.facebook.presto.operator.aggregation.FloatRegressionAggregation;
import com.facebook.presto.operator.aggregation.FloatSumAggregation;
import com.facebook.presto.operator.aggregation.GeometricMeanAggregations;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.aggregation.LongSumAggregation;
import com.facebook.presto.operator.aggregation.MergeHyperLogLogAggregation;
import com.facebook.presto.operator.aggregation.VarianceAggregation;
import com.facebook.presto.operator.scalar.ArrayCardinalityFunction;
import com.facebook.presto.operator.scalar.ArrayConcatFunction;
import com.facebook.presto.operator.scalar.ArrayContains;
import com.facebook.presto.operator.scalar.ArrayDistinctFunction;
import com.facebook.presto.operator.scalar.ArrayElementAtFunction;
import com.facebook.presto.operator.scalar.ArrayEqualOperator;
import com.facebook.presto.operator.scalar.ArrayFunctions;
import com.facebook.presto.operator.scalar.ArrayGreaterThanOperator;
import com.facebook.presto.operator.scalar.ArrayGreaterThanOrEqualOperator;
import com.facebook.presto.operator.scalar.ArrayHashCodeOperator;
import com.facebook.presto.operator.scalar.ArrayIntersectFunction;
import com.facebook.presto.operator.scalar.ArrayLessThanOrEqualOperator;
import com.facebook.presto.operator.scalar.ArrayMaxFunction;
import com.facebook.presto.operator.scalar.ArrayMinFunction;
import com.facebook.presto.operator.scalar.ArrayNotEqualOperator;
import com.facebook.presto.operator.scalar.ArrayPositionFunction;
import com.facebook.presto.operator.scalar.ArrayRemoveFunction;
import com.facebook.presto.operator.scalar.ArraySliceFunction;
import com.facebook.presto.operator.scalar.ArraySortFunction;
import com.facebook.presto.operator.scalar.BitwiseFunctions;
import com.facebook.presto.operator.scalar.ColorFunctions;
import com.facebook.presto.operator.scalar.CombineHashFunction;
import com.facebook.presto.operator.scalar.DateTimeFunctions;
import com.facebook.presto.operator.scalar.FailureFunction;
import com.facebook.presto.operator.scalar.FilterPredicatePushDownFunction;
import com.facebook.presto.operator.scalar.HyperLogLogFunctions;
import com.facebook.presto.operator.scalar.JoniRegexpFunctions;
import com.facebook.presto.operator.scalar.JsonFunctions;
import com.facebook.presto.operator.scalar.JsonOperators;
import com.facebook.presto.operator.scalar.MapCardinalityFunction;
import com.facebook.presto.operator.scalar.MapConcatFunction;
import com.facebook.presto.operator.scalar.MapEqualOperator;
import com.facebook.presto.operator.scalar.MapKeys;
import com.facebook.presto.operator.scalar.MapNotEqualOperator;
import com.facebook.presto.operator.scalar.MapToMapCast;
import com.facebook.presto.operator.scalar.MapValues;
import com.facebook.presto.operator.scalar.MathFunctions;
import com.facebook.presto.operator.scalar.Re2JCastToRegexpFunction;
import com.facebook.presto.operator.scalar.Re2JRegexpFunctions;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.SequenceFunction;
import com.facebook.presto.operator.scalar.StringFunctions;
import com.facebook.presto.operator.scalar.UrlFunctions;
import com.facebook.presto.operator.scalar.VarbinaryFunctions;
import com.facebook.presto.operator.window.CumulativeDistributionFunction;
import com.facebook.presto.operator.window.DenseRankFunction;
import com.facebook.presto.operator.window.FirstValueFunction;
import com.facebook.presto.operator.window.LagFunction;
import com.facebook.presto.operator.window.LastValueFunction;
import com.facebook.presto.operator.window.LeadFunction;
import com.facebook.presto.operator.window.NTileFunction;
import com.facebook.presto.operator.window.NthValueFunction;
import com.facebook.presto.operator.window.PercentRankFunction;
import com.facebook.presto.operator.window.RankFunction;
import com.facebook.presto.operator.window.RowNumberFunction;
import com.facebook.presto.operator.window.SqlWindowFunction;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.BigintOperators;
import com.facebook.presto.type.BooleanOperators;
import com.facebook.presto.type.ColorOperators;
import com.facebook.presto.type.DateOperators;
import com.facebook.presto.type.DateTimeOperators;
import com.facebook.presto.type.DecimalOperators;
import com.facebook.presto.type.DoubleOperators;
import com.facebook.presto.type.FloatOperators;
import com.facebook.presto.type.HyperLogLogOperators;
import com.facebook.presto.type.IntegerOperators;
import com.facebook.presto.type.IntervalDayTimeOperators;
import com.facebook.presto.type.IntervalYearMonthOperators;
import com.facebook.presto.type.LikeFunctions;
import com.facebook.presto.type.SmallintOperators;
import com.facebook.presto.type.TimeOperators;
import com.facebook.presto.type.TimeWithTimeZoneOperators;
import com.facebook.presto.type.TimestampOperators;
import com.facebook.presto.type.TimestampWithTimeZoneOperators;
import com.facebook.presto.type.TinyintOperators;
import com.facebook.presto.type.UnknownOperators;
import com.facebook.presto.type.VarbinaryOperators;
import com.facebook.presto.type.VarcharOperators;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Primitives;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.slice.Slice;

import javax.annotation.concurrent.ThreadSafe;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.metadata.FunctionKind.APPROXIMATE_AGGREGATE;
import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.FunctionKind.WINDOW;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.operator.aggregation.ArbitraryAggregationFunction.ARBITRARY_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ChecksumAggregationFunction.CHECKSUM_AGGREGATION;
import static com.facebook.presto.operator.aggregation.CountColumn.COUNT_COLUMN;
import static com.facebook.presto.operator.aggregation.Histogram.HISTOGRAM;
import static com.facebook.presto.operator.aggregation.MapAggregationFunction.MAP_AGG;
import static com.facebook.presto.operator.aggregation.MapUnionAggregation.MAP_UNION;
import static com.facebook.presto.operator.aggregation.MaxAggregationFunction.MAX_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MaxBy.MAX_BY;
import static com.facebook.presto.operator.aggregation.MaxByNAggregationFunction.MAX_BY_N_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MaxNAggregationFunction.MAX_N_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MinAggregationFunction.MIN_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MinBy.MIN_BY;
import static com.facebook.presto.operator.aggregation.MinByNAggregationFunction.MIN_BY_N_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MinNAggregationFunction.MIN_N_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MultimapAggregationFunction.MULTIMAP_AGG;
import static com.facebook.presto.operator.scalar.ArrayConstructor.ARRAY_CONSTRUCTOR;
import static com.facebook.presto.operator.scalar.ArrayFlattenFunction.ARRAY_FLATTEN_FUNCTION;
import static com.facebook.presto.operator.scalar.ArrayJoin.ARRAY_JOIN;
import static com.facebook.presto.operator.scalar.ArrayJoin.ARRAY_JOIN_WITH_NULL_REPLACEMENT;
import static com.facebook.presto.operator.scalar.ArrayLessThanOperator.ARRAY_LESS_THAN;
import static com.facebook.presto.operator.scalar.ArraySubscriptOperator.ARRAY_SUBSCRIPT;
import static com.facebook.presto.operator.scalar.ArrayToArrayCast.ARRAY_TO_ARRAY_CAST;
import static com.facebook.presto.operator.scalar.ArrayToElementConcatFunction.ARRAY_TO_ELEMENT_CONCAT_FUNCTION;
import static com.facebook.presto.operator.scalar.ArrayToJsonCast.ARRAY_TO_JSON;
import static com.facebook.presto.operator.scalar.CastFromUnknownOperator.CAST_FROM_UNKNOWN;
import static com.facebook.presto.operator.scalar.ConcatFunction.CONCAT;
import static com.facebook.presto.operator.scalar.ElementToArrayConcatFunction.ELEMENT_TO_ARRAY_CONCAT_FUNCTION;
import static com.facebook.presto.operator.scalar.Greatest.GREATEST;
import static com.facebook.presto.operator.scalar.IdentityCast.IDENTITY_CAST;
import static com.facebook.presto.operator.scalar.JsonToArrayCast.JSON_TO_ARRAY;
import static com.facebook.presto.operator.scalar.JsonToMapCast.JSON_TO_MAP;
import static com.facebook.presto.operator.scalar.Least.LEAST;
import static com.facebook.presto.operator.scalar.MapConstructor.MAP_CONSTRUCTOR;
import static com.facebook.presto.operator.scalar.MapElementAtFunction.MAP_ELEMENT_AT;
import static com.facebook.presto.operator.scalar.MapHashCodeOperator.MAP_HASH_CODE;
import static com.facebook.presto.operator.scalar.MapSubscriptOperator.MAP_SUBSCRIPT;
import static com.facebook.presto.operator.scalar.MapToJsonCast.MAP_TO_JSON;
import static com.facebook.presto.operator.scalar.RowEqualOperator.ROW_EQUAL;
import static com.facebook.presto.operator.scalar.RowHashCodeOperator.ROW_HASH_CODE;
import static com.facebook.presto.operator.scalar.RowNotEqualOperator.ROW_NOT_EQUAL;
import static com.facebook.presto.operator.scalar.RowToJsonCast.ROW_TO_JSON;
import static com.facebook.presto.operator.scalar.RowToRowCast.ROW_TO_ROW_CAST;
import static com.facebook.presto.operator.scalar.TryCastFunction.TRY_CAST;
import static com.facebook.presto.operator.scalar.VarcharToVarcharCast.VARCHAR_TO_VARCHAR_CAST;
import static com.facebook.presto.operator.scalar.ZipFunction.ZIP_FUNCTIONS;
import static com.facebook.presto.operator.window.AggregateWindowFunction.supplier;
import static com.facebook.presto.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_CALL;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.type.DecimalCasts.BIGINT_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.BOOLEAN_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_BIGINT_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_BOOLEAN_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_DOUBLE_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_FLOAT_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_INTEGER_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_JSON_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_SMALLINT_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_TINYINT_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_VARCHAR_CAST;
import static com.facebook.presto.type.DecimalCasts.DOUBLE_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.FLOAT_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.INTEGER_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.JSON_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.SMALLINT_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.TINYINT_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.VARCHAR_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_BETWEEN_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_EQUAL_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_GREATER_THAN_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_LESS_THAN_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_NOT_EQUAL_OPERATOR;
import static com.facebook.presto.type.DecimalOperators.DECIMAL_ADD_OPERATOR;
import static com.facebook.presto.type.DecimalOperators.DECIMAL_DIVIDE_OPERATOR;
import static com.facebook.presto.type.DecimalOperators.DECIMAL_MODULUS_OPERATOR;
import static com.facebook.presto.type.DecimalOperators.DECIMAL_MULTIPLY_OPERATOR;
import static com.facebook.presto.type.DecimalOperators.DECIMAL_SUBTRACT_OPERATOR;
import static com.facebook.presto.type.DecimalSaturatedFloorCasts.DECIMAL_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static com.facebook.presto.type.DecimalToDecimalCasts.DECIMAL_TO_DECIMAL_CAST;
import static com.facebook.presto.type.TypeUtils.resolveTypes;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class FunctionRegistry
{
    private static final String MAGIC_LITERAL_FUNCTION_PREFIX = "$literal$";
    private static final String OPERATOR_PREFIX = "$operator$";

    // hack: java classes for types that can be used with magic literals
    private static final Set<Class<?>> SUPPORTED_LITERAL_TYPES = ImmutableSet.<Class<?>>of(long.class, double.class, Slice.class, boolean.class);

    private final TypeManager typeManager;
    private final BlockEncodingSerde blockEncodingSerde;
    private final LoadingCache<SpecializedFunctionKey, ScalarFunctionImplementation> specializedScalarCache;
    private final LoadingCache<SpecializedFunctionKey, InternalAggregationFunction> specializedAggregationCache;
    private final LoadingCache<SpecializedFunctionKey, WindowFunctionSupplier> specializedWindowCache;
    private volatile FunctionMap functions = new FunctionMap();

    public FunctionRegistry(TypeManager typeManager, BlockEncodingSerde blockEncodingSerde, FeaturesConfig featuresConfig)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");

        specializedScalarCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(new CacheLoader<SpecializedFunctionKey, ScalarFunctionImplementation>()
                {
                    @Override
                    public ScalarFunctionImplementation load(SpecializedFunctionKey key)
                            throws Exception
                    {
                        // TODO the function map should be updated, so that this cast can be removed
                        SqlScalarFunction scalarFunction = checkType(key.getFunction(), SqlScalarFunction.class, "function");
                        return scalarFunction.specialize(key.getBoundVariables(), key.getArity(), typeManager, FunctionRegistry.this);
                    }
                });

        specializedAggregationCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(new CacheLoader<SpecializedFunctionKey, InternalAggregationFunction>()
                {
                    @Override
                    public InternalAggregationFunction load(SpecializedFunctionKey key)
                            throws Exception
                    {
                        SqlAggregationFunction aggregationFunction = checkType(key.getFunction(), SqlAggregationFunction.class, "function");
                        return aggregationFunction.specialize(key.getBoundVariables(), key.getArity(), typeManager, FunctionRegistry.this);
                    }
                });

        specializedWindowCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(new CacheLoader<SpecializedFunctionKey, WindowFunctionSupplier>()
                {
                    @Override
                    public WindowFunctionSupplier load(SpecializedFunctionKey key)
                            throws Exception
                    {
                        if (key.getFunction() instanceof SqlAggregationFunction) {
                            SqlAggregationFunction aggregationFunction = checkType(key.getFunction(), SqlAggregationFunction.class, "function");
                            return supplier(aggregationFunction.getSignature(), specializedAggregationCache.getUnchecked(key));
                        }
                        else {
                            SqlWindowFunction windowFunction = checkType(key.getFunction(), SqlWindowFunction.class, "function");
                            return windowFunction.specialize(key.getBoundVariables(), key.getArity(), typeManager, FunctionRegistry.this);
                        }
                    }
                });

        FunctionListBuilder builder = new FunctionListBuilder()
                .window(RowNumberFunction.class)
                .window(RankFunction.class)
                .window(DenseRankFunction.class)
                .window(PercentRankFunction.class)
                .window(CumulativeDistributionFunction.class)
                .window(NTileFunction.class)
                .window(FirstValueFunction.class)
                .window(LastValueFunction.class)
                .window(NthValueFunction.class)
                .window(LagFunction.class)
                .window(LeadFunction.class)
                .aggregate(CountAggregation.class)
                .aggregate(VarianceAggregation.class)
                .aggregate(ApproximateLongPercentileAggregations.class)
                .aggregate(ApproximateLongPercentileArrayAggregations.class)
                .aggregate(ApproximateDoublePercentileAggregations.class)
                .aggregate(ApproximateDoublePercentileArrayAggregations.class)
                .aggregate(ApproximateFloatPercentileAggregations.class)
                .aggregate(ApproximateFloatPercentileArrayAggregations.class)
                .aggregate(CountIfAggregation.class)
                .aggregate(BooleanAndAggregation.class)
                .aggregate(BooleanOrAggregation.class)
                .aggregate(DoubleSumAggregation.class)
                .aggregate(FloatSumAggregation.class)
                .aggregate(LongSumAggregation.class)
                .aggregate(AverageAggregations.class)
                .aggregate(FloatAverageAggregation.class)
                .aggregate(GeometricMeanAggregations.class)
                .aggregate(FloatGeometricMeanAggregations.class)
                .aggregate(ApproximateCountDistinctAggregations.class)
                .aggregate(MergeHyperLogLogAggregation.class)
                .aggregate(ApproximateSetAggregation.class)
                .aggregate(DoubleHistogramAggregation.class)
                .aggregate(FloatHistogramAggregation.class)
                .aggregate(DoubleCovarianceAggregation.class)
                .aggregate(FloatCovarianceAggregation.class)
                .aggregate(DoubleRegressionAggregation.class)
                .aggregate(FloatRegressionAggregation.class)
                .aggregate(DoubleCorrelationAggregation.class)
                .aggregate(FloatCorrelationAggregation.class)
                .scalars(SequenceFunction.class)
                .scalars(StringFunctions.class)
                .scalars(VarbinaryFunctions.class)
                .scalars(UrlFunctions.class)
                .scalars(MathFunctions.class)
                .scalars(BitwiseFunctions.class)
                .scalars(DateTimeFunctions.class)
                .scalars(JsonFunctions.class)
                .scalars(ColorFunctions.class)
                .scalars(ColorOperators.class)
                .scalars(HyperLogLogFunctions.class)
                .scalars(UnknownOperators.class)
                .scalars(BooleanOperators.class)
                .scalars(BigintOperators.class)
                .scalars(IntegerOperators.class)
                .scalars(SmallintOperators.class)
                .scalars(TinyintOperators.class)
                .scalars(DoubleOperators.class)
                .scalars(FloatOperators.class)
                .scalars(VarcharOperators.class)
                .scalars(VarbinaryOperators.class)
                .scalars(DateOperators.class)
                .scalars(TimeOperators.class)
                .scalars(TimestampOperators.class)
                .scalars(IntervalDayTimeOperators.class)
                .scalars(IntervalYearMonthOperators.class)
                .scalars(TimeWithTimeZoneOperators.class)
                .scalars(TimestampWithTimeZoneOperators.class)
                .scalars(DateTimeOperators.class)
                .scalars(HyperLogLogOperators.class)
                .scalars(LikeFunctions.class)
                .scalars(ArrayFunctions.class)
                .scalar(ArrayCardinalityFunction.class)
                .scalar(ArrayContains.class)
                .scalar(ArrayPositionFunction.class)
                .scalars(CombineHashFunction.class)
                .scalars(JsonOperators.class)
                .scalars(FailureFunction.class)
                .scalars(FilterPredicatePushDownFunction.class)
                .scalar(DecimalOperators.Negation.class)
                .scalar(DecimalOperators.HashCode.class)
                .functions(IDENTITY_CAST, CAST_FROM_UNKNOWN)
                .scalar(ArrayLessThanOrEqualOperator.class)
                .scalar(ArrayRemoveFunction.class)
                .scalar(ArrayGreaterThanOperator.class)
                .scalar(ArrayGreaterThanOrEqualOperator.class)
                .scalar(ArrayElementAtFunction.class)
                .scalar(ArraySortFunction.class)
                .scalar(ArrayMinFunction.class)
                .scalar(ArrayMaxFunction.class)
                .scalar(ArrayDistinctFunction.class)
                .scalar(ArrayConcatFunction.class)
                .scalar(ArrayNotEqualOperator.class)
                .scalar(ArrayEqualOperator.class)
                .scalar(ArrayHashCodeOperator.class)
                .scalar(ArrayIntersectFunction.class)
                .scalar(ArraySliceFunction.class)
                .scalar(MapEqualOperator.class)
                .scalar(MapNotEqualOperator.class)
                .scalar(MapKeys.class)
                .scalar(MapValues.class)
                .scalar(MapCardinalityFunction.class)
                .scalar(MapConcatFunction.class)
                .scalar(MapToMapCast.class)
                .functions(ZIP_FUNCTIONS)
                .functions(ARRAY_JOIN, ARRAY_JOIN_WITH_NULL_REPLACEMENT)
                .functions(ARRAY_TO_ARRAY_CAST, ARRAY_LESS_THAN)
                .functions(ARRAY_TO_ELEMENT_CONCAT_FUNCTION, ELEMENT_TO_ARRAY_CONCAT_FUNCTION)
                .function(MAP_HASH_CODE)
                .function(MAP_ELEMENT_AT)
                .function(ARRAY_FLATTEN_FUNCTION)
                .functions(ARRAY_CONSTRUCTOR, ARRAY_SUBSCRIPT, ARRAY_TO_JSON, JSON_TO_ARRAY)
                .functions(MAP_CONSTRUCTOR, MAP_SUBSCRIPT, MAP_TO_JSON, JSON_TO_MAP)
                .functions(MAP_AGG, MULTIMAP_AGG, MAP_UNION)
                .functions(DECIMAL_TO_VARCHAR_CAST, DECIMAL_TO_INTEGER_CAST, DECIMAL_TO_BIGINT_CAST, DECIMAL_TO_DOUBLE_CAST, DECIMAL_TO_FLOAT_CAST, DECIMAL_TO_BOOLEAN_CAST, DECIMAL_TO_TINYINT_CAST, DECIMAL_TO_SMALLINT_CAST)
                .functions(VARCHAR_TO_DECIMAL_CAST, INTEGER_TO_DECIMAL_CAST, BIGINT_TO_DECIMAL_CAST, DOUBLE_TO_DECIMAL_CAST, FLOAT_TO_DECIMAL_CAST, BOOLEAN_TO_DECIMAL_CAST, TINYINT_TO_DECIMAL_CAST, SMALLINT_TO_DECIMAL_CAST)
                .functions(JSON_TO_DECIMAL_CAST, DECIMAL_TO_JSON_CAST)
                .functions(DECIMAL_ADD_OPERATOR, DECIMAL_SUBTRACT_OPERATOR, DECIMAL_MULTIPLY_OPERATOR, DECIMAL_DIVIDE_OPERATOR, DECIMAL_MODULUS_OPERATOR)
                .functions(DECIMAL_EQUAL_OPERATOR, DECIMAL_NOT_EQUAL_OPERATOR)
                .functions(DECIMAL_LESS_THAN_OPERATOR, DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR)
                .functions(DECIMAL_GREATER_THAN_OPERATOR, DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR)
                .functions(DECIMAL_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .function(DECIMAL_BETWEEN_OPERATOR)
                .function(HISTOGRAM)
                .function(CHECKSUM_AGGREGATION)
                .function(VARCHAR_TO_VARCHAR_CAST)
                .function(IDENTITY_CAST)
                .function(ARBITRARY_AGGREGATION)
                .functions(GREATEST, LEAST)
                .functions(MAX_BY, MIN_BY, MAX_BY_N_AGGREGATION, MIN_BY_N_AGGREGATION)
                .functions(MAX_AGGREGATION, MIN_AGGREGATION, MAX_N_AGGREGATION, MIN_N_AGGREGATION)
                .function(COUNT_COLUMN)
                .functions(ROW_HASH_CODE, ROW_TO_JSON, ROW_EQUAL, ROW_NOT_EQUAL, ROW_TO_ROW_CAST)
                .function(CONCAT)
                .function(DECIMAL_TO_DECIMAL_CAST)
                .function(TRY_CAST);

        builder.function(new ArrayAggregationFunction(featuresConfig.isLegacyArrayAgg()));

        switch (featuresConfig.getRegexLibrary()) {
            case JONI:
                builder.scalars(JoniRegexpFunctions.class);
                break;
            case RE2J:
                builder.scalars(Re2JRegexpFunctions.class)
                        .function(new Re2JCastToRegexpFunction(featuresConfig.getRe2JDfaStatesLimit(), featuresConfig.getRe2JDfaRetries()));
                break;
        }

        if (featuresConfig.isExperimentalSyntaxEnabled()) {
            builder.aggregate(ApproximateAverageAggregations.class)
                    .aggregate(ApproximateSumAggregations.class)
                    .aggregate(ApproximateCountAggregation.class)
                    .aggregate(ApproximateCountColumnAggregations.class);
        }

        addFunctions(builder.getFunctions());
    }

    public final synchronized void addFunctions(List<? extends SqlFunction> functions)
    {
        for (SqlFunction function : functions) {
            for (SqlFunction existingFunction : this.functions.list()) {
                checkArgument(!function.getSignature().equals(existingFunction.getSignature()), "Function already registered: %s", function.getSignature());
            }
        }
        this.functions = new FunctionMap(this.functions, functions);
    }

    public List<SqlFunction> list()
    {
        return functions.list().stream()
                .filter(function -> !function.isHidden())
                .collect(toImmutableList());
    }

    public boolean isAggregationFunction(QualifiedName name)
    {
        return Iterables.any(functions.get(name), function -> function.getSignature().getKind() == AGGREGATE || function.getSignature().getKind() == APPROXIMATE_AGGREGATE);
    }

    public Signature resolveFunction(QualifiedName name, List<TypeSignature> parameterTypes, boolean approximate)
    {
        List<SqlFunction> allCandidates = functions.get(name).stream()
                .filter(function -> function.getSignature().getKind() == SCALAR || (function.getSignature().getKind() == APPROXIMATE_AGGREGATE) == approximate)
                .collect(toImmutableList());

        List<SqlFunction> exactCandidates = allCandidates.stream()
                .filter(function -> function.getSignature().getTypeVariableConstraints().isEmpty())
                .collect(Collectors.toList());

        List<Type> resolvedTypes = resolveTypes(parameterTypes, typeManager);

        Optional<Signature> match = matchFunctionExact(exactCandidates, resolvedTypes);
        if (match.isPresent()) {
            return match.get();
        }

        List<SqlFunction> genericCandidates = allCandidates.stream()
                .filter(function -> !function.getSignature().getTypeVariableConstraints().isEmpty())
                .collect(Collectors.toList());

        match = matchFunctionExact(genericCandidates, resolvedTypes);
        if (match.isPresent()) {
            return match.get();
        }

        match = matchFunctionWithCoercion(allCandidates, resolvedTypes);
        if (match.isPresent()) {
            return match.get();
        }

        List<String> expectedParameters = new ArrayList<>();
        for (SqlFunction function : allCandidates) {
            expectedParameters.add(format("%s(%s) %s",
                    name,
                    Joiner.on(", ").join(function.getSignature().getArgumentTypes()),
                    Joiner.on(", ").join(function.getSignature().getTypeVariableConstraints())));
        }
        String parameters = Joiner.on(", ").join(parameterTypes);
        String message = format("Function %s not registered", name);
        if (!expectedParameters.isEmpty()) {
            String expected = Joiner.on(", ").join(expectedParameters);
            message = format("Unexpected parameters (%s) for function %s. Expected: %s", parameters, name, expected);
        }

        if (name.getSuffix().startsWith(MAGIC_LITERAL_FUNCTION_PREFIX)) {
            // extract type from function name
            String typeName = name.getSuffix().substring(MAGIC_LITERAL_FUNCTION_PREFIX.length());

            // lookup the type
            Type type = typeManager.getType(parseTypeSignature(typeName));
            requireNonNull(type, format("Type %s not registered", typeName));

            // verify we have one parameter of the proper type
            checkArgument(parameterTypes.size() == 1, "Expected one argument to literal function, but got %s", parameterTypes);
            Type parameterType = typeManager.getType(parameterTypes.get(0));
            requireNonNull(parameterType, format("Type %s not found", parameterTypes.get(0)));

            return getMagicLiteralFunctionSignature(type);
        }

        throw new PrestoException(FUNCTION_NOT_FOUND, message);
    }

    private Optional<Signature> matchFunctionExact(List<SqlFunction> candidates, List<Type> actualParameters)
    {
        return matchFunction(candidates, actualParameters, false);
    }

    private Optional<Signature> matchFunctionWithCoercion(List<SqlFunction> candidates, List<Type> actualParameters)
    {
        return matchFunction(candidates, actualParameters, true);
    }

    private Optional<Signature> matchFunction(List<SqlFunction> candidates, List<Type> parameters, boolean coercionAllowed)
    {
        List<ApplicableFunction> applicableFunctions = identifyApplicableFunctions(candidates, parameters, coercionAllowed);
        if (applicableFunctions.isEmpty()) {
            return Optional.empty();
        }

        if (coercionAllowed) {
            applicableFunctions = selectMostSpecificFunctions(applicableFunctions, parameters);
            checkState(!applicableFunctions.isEmpty(), "at least single function must be left");
        }

        if (applicableFunctions.size() == 1) {
            return Optional.of(getOnlyElement(applicableFunctions).getBoundSignature());
        }

        StringBuilder errorMessageBuilder = new StringBuilder();
        errorMessageBuilder.append("Could not choose a best candidate operator. Explicit type casts must be added.\n");
        errorMessageBuilder.append("Candidates are:\n");
        for (ApplicableFunction function : applicableFunctions) {
            errorMessageBuilder.append("\t * ");
            errorMessageBuilder.append(function.getBoundSignature().toString());
            errorMessageBuilder.append("\n");
        }
        throw new PrestoException(AMBIGUOUS_FUNCTION_CALL, errorMessageBuilder.toString());
    }

    private List<ApplicableFunction> identifyApplicableFunctions(List<SqlFunction> candidates, List<Type> actualParameters, boolean allowCoercion)
    {
        ImmutableList.Builder<ApplicableFunction> applicableFunctions = ImmutableList.builder();
        for (SqlFunction function : candidates) {
            Signature declaredSignature = function.getSignature();
            Optional<Signature> boundSignature = new SignatureBinder(typeManager, declaredSignature, allowCoercion)
                    .bind(actualParameters);
            if (boundSignature.isPresent()) {
                applicableFunctions.add(new ApplicableFunction(declaredSignature, boundSignature.get()));
            }
        }
        return applicableFunctions.build();
    }

    private List<ApplicableFunction> selectMostSpecificFunctions(List<ApplicableFunction> applicableFunctions, List<Type> parameters)
    {
        List<ApplicableFunction> mostSpecificFunctions = selectMostSpecificFunctions(applicableFunctions);
        if (mostSpecificFunctions.size() > 1 && someParameterIsUnknown(parameters)) {
            // if there is more than one function, look for functions that only cast the unknown arguments
            List<ApplicableFunction> unknownOnlyCastFunctions = getUnknownOnlyCastFunctions(applicableFunctions, parameters);
            if (!unknownOnlyCastFunctions.isEmpty()) {
                mostSpecificFunctions = unknownOnlyCastFunctions;
            }
            if (mostSpecificFunctions.size() == 1) {
                return mostSpecificFunctions;
            }
            // If the return type for all the selected function is the same, and the parameters are not declared as nullable
            // all the functions are semantically the same. We can return just any of those.
            if (returnTypeIsTheSame(mostSpecificFunctions) && allReturnNullOnGivenInputTypes(mostSpecificFunctions, parameters)) {
                // make it deterministic
                ApplicableFunction selectedFunction = Ordering.usingToString()
                        .reverse()
                        .sortedCopy(mostSpecificFunctions)
                        .get(0);
                return ImmutableList.of(selectedFunction);
            }
        }
        return mostSpecificFunctions;
    }

    private List<ApplicableFunction> selectMostSpecificFunctions(List<ApplicableFunction> candidates)
    {
        List<ApplicableFunction> representatives = new ArrayList<>();

        for (ApplicableFunction current : candidates) {
            boolean found = false;
            for (int i = 0; i < representatives.size(); i++) {
                ApplicableFunction representative = representatives.get(i);
                if (isMoreSpecificThan(current, representative)) {
                    representatives.set(i, current);
                }
                if (isMoreSpecificThan(current, representative) || isMoreSpecificThan(representative, current)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                representatives.add(current);
            }
        }

        return representatives;
    }

    private boolean someParameterIsUnknown(List<Type> parameters)
    {
        return parameters.stream().anyMatch(type -> type.equals(UNKNOWN));
    }

    private List<ApplicableFunction> getUnknownOnlyCastFunctions(List<ApplicableFunction> applicableFunction, List<Type> actualParameters)
    {
        return applicableFunction.stream()
                .filter((function) -> onlyCastsUnknown(function, actualParameters))
                .collect(toImmutableList());
    }

    private boolean onlyCastsUnknown(ApplicableFunction applicableFunction, List<Type> actualParameters)
    {
        List<Type> boundTypes = resolveTypes(applicableFunction.getBoundSignature().getArgumentTypes(), typeManager);
        checkState(actualParameters.size() == boundTypes.size(), "type lists are of different lengths");
        for (int i = 0; i < actualParameters.size(); i++) {
            if (!boundTypes.get(i).equals(actualParameters.get(i)) && actualParameters.get(i) != UNKNOWN) {
                return false;
            }
        }
        return true;
    }

    private boolean returnTypeIsTheSame(List<ApplicableFunction> applicableFunctions)
    {
        Set<Type> returnTypes = applicableFunctions.stream()
                .map(function -> typeManager.getType(function.getBoundSignature().getReturnType()))
                .collect(Collectors.toSet());
        return returnTypes.size() == 1;
    }

    private boolean allReturnNullOnGivenInputTypes(List<ApplicableFunction> applicableFunctions, List<Type> parameters)
    {
        return applicableFunctions.stream().allMatch(x -> returnsNullOnGivenInputTypes(x, parameters));
    }

    private boolean returnsNullOnGivenInputTypes(ApplicableFunction applicableFunction, List<Type> parameterTypes)
    {
        for (int i = 0; i < parameterTypes.size(); i++) {
            Type parameterType = parameterTypes.get(i);
            if (parameterType.equals(UNKNOWN)) {
                if (parameterIsNullable(applicableFunction.getBoundSignature(), i)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean parameterIsNullable(Signature boundSignature, int parameterIndex)
    {
        FunctionKind functionKind = boundSignature.getKind();
        // nullable parameters can be declared only for scalar functions
        // Window and Aggregation functions have fixed semantic where NULL values are always skipped
        if (functionKind != SCALAR) {
            return false;
        }
        // TODO: Move information about nullable arguments to FunctionSignature. Remove this hack.
        ScalarFunctionImplementation implementation = getScalarFunctionImplementation(boundSignature);
        return implementation.getNullableArguments().get(parameterIndex);
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(Signature signature)
    {
        checkArgument(signature.getKind() == WINDOW || signature.getKind() == AGGREGATE, "%s is not a window function", signature);
        checkArgument(signature.getTypeVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);
        Iterable<SqlFunction> candidates = functions.get(QualifiedName.of(signature.getName()));
        // search for exact match
        for (SqlFunction operator : candidates) {
            Type returnType = typeManager.getType(signature.getReturnType());
            List<Type> argumentTypes = resolveTypes(signature.getArgumentTypes(), typeManager);
            Optional<BoundVariables> boundVariables = new SignatureBinder(typeManager, operator.getSignature(), false)
                    .bindVariables(argumentTypes, returnType);
            if (boundVariables.isPresent()) {
                try {
                    return specializedWindowCache.getUnchecked(new SpecializedFunctionKey(operator, boundVariables.get(), argumentTypes.size()));
                }
                catch (UncheckedExecutionException e) {
                    throw Throwables.propagate(e.getCause());
                }
            }
        }
        throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("%s not found", signature));
    }

    public InternalAggregationFunction getAggregateFunctionImplementation(Signature signature)
    {
        checkArgument(signature.getKind() == AGGREGATE || signature.getKind() == APPROXIMATE_AGGREGATE, "%s is not an aggregate function", signature);
        checkArgument(signature.getTypeVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);
        Iterable<SqlFunction> candidates = functions.get(QualifiedName.of(signature.getName()));
        // search for exact match
        for (SqlFunction operator : candidates) {
            Type returnType = typeManager.getType(signature.getReturnType());
            List<Type> argumentTypes = resolveTypes(signature.getArgumentTypes(), typeManager);
            Optional<BoundVariables> boundVariables = new SignatureBinder(typeManager, operator.getSignature(), false)
                    .bindVariables(argumentTypes, returnType);
            if (boundVariables.isPresent()) {
                try {
                    return specializedAggregationCache.getUnchecked(new SpecializedFunctionKey(operator, boundVariables.get(), signature.getArgumentTypes().size()));
                }
                catch (UncheckedExecutionException e) {
                    throw Throwables.propagate(e.getCause());
                }
            }
        }
        throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("%s not found", signature));
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(Signature signature)
    {
        checkArgument(signature.getKind() == SCALAR, "%s is not a scalar function", signature);
        checkArgument(signature.getTypeVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);
        Iterable<SqlFunction> candidates = functions.get(QualifiedName.of(signature.getName()));
        // search for exact match
        Type returnType = typeManager.getType(signature.getReturnType());
        List<Type> argumentTypes = resolveTypes(signature.getArgumentTypes(), typeManager);
        for (SqlFunction operator : candidates) {
            Optional<BoundVariables> boundVariables = new SignatureBinder(typeManager, operator.getSignature(), false)
                    .bindVariables(argumentTypes, returnType);
            if (boundVariables.isPresent()) {
                try {
                    return specializedScalarCache.getUnchecked(new SpecializedFunctionKey(operator, boundVariables.get(), argumentTypes.size()));
                }
                catch (UncheckedExecutionException e) {
                    throw Throwables.propagate(e.getCause());
                }
            }
        }

        // TODO: this is a hack and should be removed
        if (signature.getName().startsWith(MAGIC_LITERAL_FUNCTION_PREFIX)) {
            List<TypeSignature> parameterTypes = signature.getArgumentTypes();
            // extract type from function name
            String typeName = signature.getName().substring(MAGIC_LITERAL_FUNCTION_PREFIX.length());

            // lookup the type
            Type type = typeManager.getType(parseTypeSignature(typeName));
            requireNonNull(type, format("Type %s not registered", typeName));

            // verify we have one parameter of the proper type
            checkArgument(parameterTypes.size() == 1, "Expected one argument to literal function, but got %s", parameterTypes);
            Type parameterType = typeManager.getType(parameterTypes.get(0));
            requireNonNull(parameterType, format("Type %s not found", parameterTypes.get(0)));

            MethodHandle methodHandle = null;
            if (parameterType.getJavaType() == type.getJavaType()) {
                methodHandle = MethodHandles.identity(parameterType.getJavaType());
            }

            if (parameterType.getJavaType() == Slice.class) {
                if (type.getJavaType() == Block.class) {
                    methodHandle = BlockSerdeUtil.READ_BLOCK.bindTo(blockEncodingSerde);
                }
            }

            checkArgument(methodHandle != null,
                    "Expected type %s to use (or can be converted into) Java type %s, but Java type is %s",
                    type,
                    parameterType.getJavaType(),
                    type.getJavaType());

            return new ScalarFunctionImplementation(false, ImmutableList.of(false), methodHandle, true);
        }

        throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("%s not found", signature));
    }

    @VisibleForTesting
    public List<SqlFunction> listOperators()
    {
        Set<String> operatorNames = Arrays.asList(OperatorType.values()).stream()
                .map(FunctionRegistry::mangleOperatorName)
                .collect(toImmutableSet());

        return functions.list().stream()
                .filter(function -> operatorNames.contains(function.getSignature().getName()))
                .collect(toImmutableList());
    }

    public boolean canResolveOperator(OperatorType operatorType, Type returnType, List<? extends Type> argumentTypes)
    {
        Signature signature = internalOperator(operatorType, returnType, argumentTypes);
        return isRegistered(signature);
    }

    public boolean isRegistered(Signature signature)
    {
        try {
            // TODO: this is hacky, but until the magic literal and row field reference hacks are cleaned up it's difficult to implement this.
            getScalarFunctionImplementation(signature);
            return true;
        }
        catch (PrestoException e) {
            if (e.getErrorCode().getCode() == FUNCTION_IMPLEMENTATION_MISSING.toErrorCode().getCode()) {
                return false;
            }
            throw e;
        }
    }

    public Signature resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        try {
            return resolveFunction(QualifiedName.of(mangleOperatorName(operatorType)), Lists.transform(argumentTypes, Type::getTypeSignature), false);
        }
        catch (PrestoException e) {
            if (e.getErrorCode().getCode() == FUNCTION_NOT_FOUND.toErrorCode().getCode()) {
                throw new OperatorNotFoundException(
                        operatorType,
                        argumentTypes.stream()
                                .map(Type::getTypeSignature)
                                .collect(toImmutableList()));
            }
            else {
                throw e;
            }
        }
    }

    public Signature getCoercion(Type fromType, Type toType)
    {
        return getCoercion(fromType.getTypeSignature(), toType.getTypeSignature());
    }

    public Signature getCoercion(TypeSignature fromType, TypeSignature toType)
    {
        Signature signature = internalOperator(OperatorType.CAST.name(), toType, ImmutableList.of(fromType));
        try {
            getScalarFunctionImplementation(signature);
        }
        catch (PrestoException e) {
            if (e.getErrorCode().getCode() == FUNCTION_IMPLEMENTATION_MISSING.toErrorCode().getCode()) {
                throw new OperatorNotFoundException(OperatorType.CAST, ImmutableList.of(fromType), toType);
            }
            throw e;
        }
        return signature;
    }

    public static Type typeForMagicLiteral(Type type)
    {
        Class<?> clazz = type.getJavaType();
        clazz = Primitives.unwrap(clazz);

        if (clazz == long.class) {
            return BIGINT;
        }
        if (clazz == double.class) {
            return DOUBLE;
        }
        if (!clazz.isPrimitive()) {
            if (type instanceof VarcharType) {
                return type;
            }
            else {
                return VARBINARY;
            }
        }
        if (clazz == boolean.class) {
            return BOOLEAN;
        }
        throw new IllegalArgumentException("Unhandled Java type: " + clazz.getName());
    }

    public static Signature getMagicLiteralFunctionSignature(Type type)
    {
        TypeSignature argumentType = typeForMagicLiteral(type).getTypeSignature();

        return new Signature(MAGIC_LITERAL_FUNCTION_PREFIX + type.getTypeSignature(),
                SCALAR,
                type.getTypeSignature(),
                argumentType);
    }

    public static boolean isSupportedLiteralType(Type type)
    {
        return SUPPORTED_LITERAL_TYPES.contains(type.getJavaType());
    }

    public static String mangleOperatorName(OperatorType operatorType)
    {
        return mangleOperatorName(operatorType.name());
    }

    public static String mangleOperatorName(String operatorName)
    {
        return OPERATOR_PREFIX + operatorName;
    }

    @VisibleForTesting
    public static OperatorType unmangleOperator(String mangledName)
    {
        checkArgument(mangledName.startsWith(OPERATOR_PREFIX), "%s is not a mangled operator name", mangledName);
        return OperatorType.valueOf(mangledName.substring(OPERATOR_PREFIX.length()));
    }

    /**
     * One method is more specific than another if invocation handled by the first method could be passed on to the other one
     */
    public boolean isMoreSpecificThan(ApplicableFunction left, ApplicableFunction right)
    {
        List<Type> resolvedTypes = resolveTypes(left.getBoundSignature().getArgumentTypes(), typeManager);
        Optional<BoundVariables> boundVariables = new SignatureBinder(typeManager, right.getDeclaredSignature(), true)
                .bindVariables(resolvedTypes);
        return boundVariables.isPresent();
    }

    private static class FunctionMap
    {
        private final Multimap<QualifiedName, SqlFunction> functions;

        public FunctionMap()
        {
            functions = ImmutableListMultimap.of();
        }

        public FunctionMap(FunctionMap map, Iterable<? extends SqlFunction> functions)
        {
            this.functions = ImmutableListMultimap.<QualifiedName, SqlFunction>builder()
                    .putAll(map.functions)
                    .putAll(Multimaps.index(functions, function -> QualifiedName.of(function.getSignature().getName())))
                    .build();

            // Make sure all functions with the same name are aggregations or none of them are
            for (Map.Entry<QualifiedName, Collection<SqlFunction>> entry : this.functions.asMap().entrySet()) {
                Collection<SqlFunction> values = entry.getValue();
                long aggregations = values.stream()
                        .map(function -> function.getSignature().getKind())
                        .filter(kind -> kind == AGGREGATE || kind == APPROXIMATE_AGGREGATE)
                        .count();
                checkState(aggregations == 0 || aggregations == values.size(), "'%s' is both an aggregation and a scalar function", entry.getKey());
            }
        }

        public List<SqlFunction> list()
        {
            return ImmutableList.copyOf(functions.values());
        }

        public Collection<SqlFunction> get(QualifiedName name)
        {
            return functions.get(name);
        }
    }

    private static class ApplicableFunction
    {
        private final Signature declaredSignature;
        private final Signature boundSignature;

        private ApplicableFunction(Signature declaredSignature, Signature boundSignature)
        {
            this.declaredSignature = declaredSignature;
            this.boundSignature = boundSignature;
        }

        public Signature getDeclaredSignature()
        {
            return declaredSignature;
        }

        public Signature getBoundSignature()
        {
            return boundSignature;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("declaredSignature", declaredSignature)
                    .add("boundSignature", boundSignature)
                    .toString();
        }
    }
}
