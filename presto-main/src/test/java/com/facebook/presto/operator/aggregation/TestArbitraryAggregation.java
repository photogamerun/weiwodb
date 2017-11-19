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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Set;

import static com.facebook.presto.block.BlockAssertions.createArrayBigintBlock;
import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createIntsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static org.testng.Assert.assertNotNull;

public class TestArbitraryAggregation
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testAllRegistered()
    {
        Set<Type> allTypes = metadata.getTypeManager().getTypes().stream().collect(toImmutableSet());

        for (Type valueType : allTypes) {
            assertNotNull(metadata.getFunctionRegistry().getAggregateFunctionImplementation(new Signature("arbitrary", AGGREGATE, valueType.getTypeSignature(), valueType.getTypeSignature())));
        }
    }

    @Test
    public void testNullBoolean()
            throws Exception
    {
        InternalAggregationFunction booleanAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.BOOLEAN), parseTypeSignature(StandardTypes.BOOLEAN)));
        assertAggregation(
                booleanAgg,
                1.0,
                null,
                createBooleansBlock((Boolean) null));
    }

    @Test
    public void testValidBoolean()
            throws Exception
    {
        InternalAggregationFunction booleanAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.BOOLEAN), parseTypeSignature(StandardTypes.BOOLEAN)));
        assertAggregation(
                booleanAgg,
                1.0,
                true,
                createBooleansBlock(true, true));
    }

    @Test
    public void testNullLong()
            throws Exception
    {
        InternalAggregationFunction longAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                longAgg,
                1.0,
                null,
                createLongsBlock(null, null));
    }

    @Test
    public void testValidLong()
            throws Exception
    {
        InternalAggregationFunction longAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                longAgg,
                1.0,
                1L,
                createLongsBlock(1L, null));
    }

    @Test
    public void testNullDouble()
            throws Exception
    {
        InternalAggregationFunction doubleAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.DOUBLE), parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                doubleAgg,
                1.0,
                null,
                createDoublesBlock(null, null));
    }

    @Test
    public void testValidDouble()
            throws Exception
    {
        InternalAggregationFunction doubleAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.DOUBLE), parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                doubleAgg,
                1.0,
                2.0,
                createDoublesBlock(null, 2.0));
    }

    @Test
    public void testNullString()
            throws Exception
    {
        InternalAggregationFunction stringAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR)));
        assertAggregation(
                stringAgg,
                1.0,
                null,
                createStringsBlock(null, null));
    }

    @Test
    public void testValidString()
            throws Exception
    {
        InternalAggregationFunction stringAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR)));
        assertAggregation(
                stringAgg,
                1.0,
                "a",
                createStringsBlock("a", "a"));
    }

    @Test
    public void testNullArray()
            throws Exception
    {
        InternalAggregationFunction arrayAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)")));
        assertAggregation(
                arrayAgg,
                1.0,
                null,
                createArrayBigintBlock(Arrays.asList(null, null, null, null)));
    }

    @Test
    public void testValidArray()
            throws Exception
    {
        InternalAggregationFunction arrayAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)")));
        assertAggregation(
                arrayAgg,
                1.0,
                ImmutableList.of(23L, 45L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L))));
    }

    @Test
    public void testValidInt()
            throws Exception
    {
        InternalAggregationFunction arrayAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature("integer"), parseTypeSignature("integer")));
        assertAggregation(
                arrayAgg,
                1.0,
                3,
                createIntsBlock(3, 3, null));
    }
}
