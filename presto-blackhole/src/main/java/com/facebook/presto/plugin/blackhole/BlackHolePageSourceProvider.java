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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.plugin.blackhole.Types.checkType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkArgument;

public final class BlackHolePageSourceProvider
        implements ConnectorPageSourceProvider
{
    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            List<ColumnHandle> columns)
    {
        BlackHoleSplit blackHoleSplit = checkType(split, BlackHoleSplit.class, "BlackHoleSplit");

        ImmutableList.Builder<Type> builder = ImmutableList.builder();

        for (ColumnHandle column : columns) {
            builder.add((checkType(column, BlackHoleColumnHandle.class, "BlackHoleColumnHandle")).getColumnType());
        }
        List<Type> types = builder.build();

        return new FixedPageSource(Iterables.limit(
                Iterables.cycle(generateZeroPage(types, blackHoleSplit.getRowsPerPage(), blackHoleSplit.getFieldsLength())),
                blackHoleSplit.getPagesCount()));
    }

    private Page generateZeroPage(List<Type> types, int rowsCount, int fieldLength)
    {
        byte[] constantBytes = new byte[fieldLength];
        Arrays.fill(constantBytes, (byte) 42);
        Slice constantSlice = Slices.wrappedBuffer(constantBytes);

        Block[] blocks = new Block[types.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = createZeroBlock(types.get(i), rowsCount, constantSlice);
        }

        return new Page(rowsCount, blocks);
    }

    private Block createZeroBlock(Type type, int rowsCount, Slice constantSlice)
    {
        checkArgument(isSupportedType(type), "Unsupported type [%s]", type);
        BlockBuilder builder;

        if (type instanceof FixedWidthType) {
            builder = type.createBlockBuilder(new BlockBuilderStatus(), rowsCount);
        }
        else {
            // do not exceed varchar limit
            if (type instanceof VarcharType) {
                if (constantSlice.length() > ((VarcharType) type).getLength()) {
                    constantSlice = constantSlice.slice(0, ((VarcharType) type).getLength());
                }
            }
            builder = type.createBlockBuilder(new BlockBuilderStatus(), rowsCount, constantSlice.length());
        }

        for (int i = 0; i < rowsCount; i++) {
            Class<?> javaType = type.getJavaType();
            if (javaType == boolean.class) {
                type.writeBoolean(builder, false);
            }
            else if (javaType == long.class) {
                type.writeLong(builder, 0);
            }
            else if (javaType == double.class) {
                type.writeDouble(builder, 0.0);
            }
            else if (javaType == Slice.class) {
                type.writeSlice(builder, constantSlice, 0, constantSlice.length());
            }
            else {
                throw new UnsupportedOperationException("Unknown javaType: " + javaType.getName());
            }
        }
        return builder.build();
    }

    private boolean isSupportedType(Type type)
    {
        return ImmutableSet.of(BIGINT, DOUBLE, BOOLEAN, DATE, TIMESTAMP, VARBINARY).contains(type)
                || type instanceof VarcharType;
    }
}
