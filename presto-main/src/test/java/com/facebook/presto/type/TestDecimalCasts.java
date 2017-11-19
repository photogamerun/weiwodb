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
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.FloatType.FLOAT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestDecimalCasts
        extends AbstractTestFunctions
{
    @Test
    public void testBooleanToDecimalCasts()
    {
        assertDecimalFunction("CAST(true AS DECIMAL(2, 0))", decimal("01"));
        assertDecimalFunction("CAST(true AS DECIMAL(3, 1))", decimal("01.0"));
        assertDecimalFunction("CAST(true AS DECIMAL)", maxPrecisionDecimal(1));
        assertDecimalFunction("CAST(true AS DECIMAL(2))", decimal("01"));
        assertDecimalFunction("CAST(false AS DECIMAL(2, 0))", decimal("00"));
        assertDecimalFunction("CAST(false AS DECIMAL(2))", decimal("00"));
        assertDecimalFunction("CAST(false AS DECIMAL)", maxPrecisionDecimal(0));

        assertDecimalFunction("CAST(true AS DECIMAL(20, 10))", decimal("0000000001.0000000000"));
        assertDecimalFunction("CAST(false AS DECIMAL(20, 10))", decimal("0000000000.0000000000"));
    }

    @Test
    public void testDecimalToBooleanCasts()
    {
        assertFunction("CAST(DECIMAL '1.1' AS BOOLEAN)", BOOLEAN, true);
        assertFunction("CAST(DECIMAL '-1.1' AS BOOLEAN)", BOOLEAN, true);
        assertFunction("CAST(DECIMAL '0.0' AS BOOLEAN)", BOOLEAN, false);

        assertFunction("CAST(DECIMAL '1234567890.1234567890' AS BOOLEAN)", BOOLEAN, true);
        assertFunction("CAST(DECIMAL '-1234567890.1234567890' AS BOOLEAN)", BOOLEAN, true);
        assertFunction("CAST(DECIMAL '0.0000000000000000000' AS BOOLEAN)", BOOLEAN, false);
    }

    @Test
    public void testBigintToDecimalCasts()
    {
        assertDecimalFunction("CAST(234 AS DECIMAL(4,1))", decimal("234.0"));
        assertDecimalFunction("CAST(234 AS DECIMAL(5,2))", decimal("234.00"));
        assertDecimalFunction("CAST(234 AS DECIMAL(4,0))", decimal("0234"));
        assertDecimalFunction("CAST(-234 AS DECIMAL(4,1))", decimal("-234.0"));
        assertDecimalFunction("CAST(0 AS DECIMAL(4,2))", decimal("00.00"));
        assertDecimalFunction("CAST(12345678901234567 AS DECIMAL(17, 0))", decimal("12345678901234567"));
        assertDecimalFunction("CAST(1234567890 AS DECIMAL(20, 10))", decimal("1234567890.0000000000"));
        assertDecimalFunction("CAST(-1234567890 AS DECIMAL(20, 10))", decimal("-1234567890.0000000000"));

        assertInvalidCast("CAST(1234567890 AS DECIMAL(17,10))", "Cannot cast INTEGER '1234567890' to DECIMAL(17, 10)");
        assertInvalidCast("CAST(123 AS DECIMAL(2,1))", "Cannot cast INTEGER '123' to DECIMAL(2, 1)");
        assertInvalidCast("CAST(-123 AS DECIMAL(2,1))", "Cannot cast INTEGER '-123' to DECIMAL(2, 1)");
        assertInvalidCast("CAST(123456789012345678 AS DECIMAL(17,1))", "Cannot cast BIGINT '123456789012345678' to DECIMAL(17, 1)");
        assertInvalidCast("CAST(12345678901 AS DECIMAL(20, 10))", "Cannot cast BIGINT '12345678901' to DECIMAL(20, 10)");
    }

    @Test
    public void testDecimalToBigintCasts()
    {
        assertFunction("CAST(DECIMAL '2.34' AS BIGINT)", BIGINT, 2L);
        assertFunction("CAST(DECIMAL '2.5' AS BIGINT)", BIGINT, 3L);
        assertFunction("CAST(DECIMAL '2.49' AS BIGINT)", BIGINT, 2L);
        assertFunction("CAST(DECIMAL '20' AS BIGINT)", BIGINT, 20L);
        assertFunction("CAST(DECIMAL '1' AS BIGINT)", BIGINT, 1L);
        assertFunction("CAST(DECIMAL '0' AS BIGINT)", BIGINT, 0L);
        assertFunction("CAST(DECIMAL '-20' AS BIGINT)", BIGINT, -20L);
        assertFunction("CAST(DECIMAL '-1' AS BIGINT)", BIGINT, -1L);
        assertFunction("CAST(DECIMAL '-2.49' AS BIGINT)", BIGINT, -2L);
        assertFunction("CAST(DECIMAL '-2.5' AS BIGINT)", BIGINT, -3L);
        assertFunction("CAST(DECIMAL '0.1234567890123456' AS BIGINT)", BIGINT, 0L);
        assertFunction("CAST(DECIMAL '0.9999999999999999' AS BIGINT)", BIGINT, 1L);

        assertFunction("CAST(DECIMAL '1234567890.1234567890' AS BIGINT)", BIGINT, 1234567890L);
        assertFunction("CAST(DECIMAL '-1234567890.1234567890' AS BIGINT)", BIGINT, -1234567890L);
        assertInvalidCast("CAST(DECIMAL '12345678901234567890' AS BIGINT)", "Cannot cast '12345678901234567890' to BIGINT");
    }

    @Test
    public void testDoubleToDecimalCasts()
    {
        assertDecimalFunction("CAST(234.0 AS DECIMAL(4,1))", decimal("234.0"));
        assertDecimalFunction("CAST(.01 AS DECIMAL(3,3))", decimal(".010"));
        assertDecimalFunction("CAST(.0 AS DECIMAL(3,3))", decimal(".000"));
        assertDecimalFunction("CAST(0 AS DECIMAL(1,0))", decimal("0"));
        assertDecimalFunction("CAST(0 AS DECIMAL(4,0))", decimal("0000"));
        assertDecimalFunction("CAST(1000 AS DECIMAL(4,0))", decimal("1000"));
        assertDecimalFunction("CAST(1000.01 AS DECIMAL(7,2))", decimal("01000.01"));
        assertDecimalFunction("CAST(-234.0 AS DECIMAL(3,0))", decimal("-234"));
        assertDecimalFunction("CAST(12345678901234567 AS DECIMAL(17,0))", decimal("12345678901234567"));
        assertDecimalFunction("CAST(-12345678901234567 AS DECIMAL(17,0))", decimal("-12345678901234567"));
        assertDecimalFunction("CAST(1234567890 AS DECIMAL(20,10))", decimal("1234567890.0000000000"));
        assertDecimalFunction("CAST(-1234567890 AS DECIMAL(20,10))", decimal("-1234567890.0000000000"));

        assertInvalidCast("CAST(234.0 AS DECIMAL(2,0))", "Cannot cast DOUBLE '234.0' to DECIMAL(2, 0)");
        assertInvalidCast("CAST(1000.01 AS DECIMAL(5,2))", "Cannot cast DOUBLE '1000.01' to DECIMAL(5, 2)");
        assertInvalidCast("CAST(-234.0 AS DECIMAL(2,0))", "Cannot cast DOUBLE '-234.0' to DECIMAL(2, 0)");
        assertInvalidCast("CAST(12345678901.1 AS DECIMAL(20, 10))", "Cannot cast DOUBLE '1.23456789011E10' to DECIMAL(20, 10)");
    }

    @Test
    public void testDecimalToDoubleCasts()
    {
        assertFunction("CAST(DECIMAL '2.34' AS DOUBLE)", DOUBLE, 2.34);
        assertFunction("CAST(DECIMAL '0' AS DOUBLE)", DOUBLE, 0.0);
        assertFunction("CAST(DECIMAL '1' AS DOUBLE)", DOUBLE, 1.0);
        assertFunction("CAST(DECIMAL '-2.49' AS DOUBLE)", DOUBLE, -2.49);

        assertFunction("CAST(DECIMAL '1234567890.1234567890' AS DOUBLE)", DOUBLE, 1234567890.1234567890);
        assertFunction("CAST(DECIMAL '-1234567890.1234567890' AS DOUBLE)", DOUBLE, -1234567890.1234567890);
    }

    @Test
    public void testFloatToDecimalCasts()
    {
        assertDecimalFunction("CAST(FLOAT '234.0' AS DECIMAL(4,1))", decimal("234.0"));
        assertDecimalFunction("CAST(FLOAT '.01' AS DECIMAL(3,3))", decimal(".010"));
        assertDecimalFunction("CAST(FLOAT '.0' AS DECIMAL(3,3))", decimal(".000"));
        assertDecimalFunction("CAST(FLOAT '0' AS DECIMAL(1,0))", decimal("0"));
        assertDecimalFunction("CAST(FLOAT '0' AS DECIMAL(4,0))", decimal("0000"));
        assertDecimalFunction("CAST(FLOAT '1000' AS DECIMAL(4,0))", decimal("1000"));
        assertDecimalFunction("CAST(FLOAT '1000.01' AS DECIMAL(7,2))", decimal("01000.01"));
        assertDecimalFunction("CAST(FLOAT '-234.0' AS DECIMAL(3,0))", decimal("-234"));
        assertDecimalFunction("CAST(FLOAT '12345678407663616' AS DECIMAL(17,0))", decimal("12345678407663616"));
        assertDecimalFunction("CAST(FLOAT '-12345678407663616' AS DECIMAL(17,0))", decimal("-12345678407663616"));
        assertDecimalFunction("CAST(FLOAT '1234567936' AS DECIMAL(20,10))", decimal("1234567936.0000000000"));
        assertDecimalFunction("CAST(FLOAT '-1234567936' AS DECIMAL(20,10))", decimal("-1234567936.0000000000"));

        assertInvalidCast("CAST(FLOAT '234.0' AS DECIMAL(2,0))", "Cannot cast FLOAT '234.0' to DECIMAL(2, 0)");
        assertInvalidCast("CAST(FLOAT '1000.01' AS DECIMAL(5,2))", "Cannot cast FLOAT '1000.01' to DECIMAL(5, 2)");
        assertInvalidCast("CAST(FLOAT '-234.0' AS DECIMAL(2,0))", "Cannot cast FLOAT '-234.0' to DECIMAL(2, 0)");
        assertInvalidCast("CAST(FLOAT '98765430784.0' AS DECIMAL(20, 10))", "Cannot cast FLOAT '9.8765431E10' to DECIMAL(20, 10)");
    }

    @Test
    public void testDecimalToFloatCasts()
    {
        assertFunction("CAST(DECIMAL '2.34' AS FLOAT)", FLOAT, 2.34f);
        assertFunction("CAST(DECIMAL '0' AS FLOAT)", FLOAT, 0.0f);
        assertFunction("CAST(DECIMAL '-0' AS FLOAT)", FLOAT, 0.0f);
        assertFunction("CAST(DECIMAL '1' AS FLOAT)", FLOAT, 1.0f);
        assertFunction("CAST(DECIMAL '-2.49' AS FLOAT)", FLOAT, -2.49f);

        assertFunction("CAST(DECIMAL '1234567890.1234567890' AS FLOAT)", FLOAT, 1234567890.1234567890f);
        assertFunction("CAST(DECIMAL '-1234567890.1234567890' AS FLOAT)", FLOAT, -1234567890.1234567890f);
    }

    @Test
    public void testVarcharToDecimalCasts()
    {
        assertDecimalFunction("CAST('234.0' AS DECIMAL(4,1))", decimal("234.0"));
        assertDecimalFunction("CAST('.01' AS DECIMAL(3,3))", decimal(".010"));
        assertDecimalFunction("CAST('.0' AS DECIMAL(3,3))", decimal(".000"));
        assertDecimalFunction("CAST('0' AS DECIMAL(1,0))", decimal("0"));
        assertDecimalFunction("CAST('0' AS DECIMAL(4,0))", decimal("0000"));
        assertDecimalFunction("CAST('1000' AS DECIMAL(4,0))", decimal("1000"));
        assertDecimalFunction("CAST('1000.01' AS DECIMAL(7,2))", decimal("01000.01"));
        assertDecimalFunction("CAST('-234.0' AS DECIMAL(3,0))", decimal("-234"));
        assertDecimalFunction("CAST('12345678901234567' AS DECIMAL(17,0))", decimal("12345678901234567"));
        assertDecimalFunction("CAST('-12345678901234567' AS DECIMAL(17,0))", decimal("-12345678901234567"));
        assertDecimalFunction("CAST('1234567890' AS DECIMAL(20,10))", decimal("1234567890.0000000000"));
        assertDecimalFunction("CAST('-1234567890' AS DECIMAL(20,10))", decimal("-1234567890.0000000000"));

        assertInvalidCast("CAST('234.0' AS DECIMAL(2,0))", "Cannot cast VARCHAR '234.0' to DECIMAL(2, 0)");
        assertInvalidCast("CAST('1000.01' AS DECIMAL(5,2))", "Cannot cast VARCHAR '1000.01' to DECIMAL(5, 2)");
        assertInvalidCast("CAST('-234.0' AS DECIMAL(2,0))", "Cannot cast VARCHAR '-234.0' to DECIMAL(2, 0)");
        assertInvalidCast("CAST('12345678901' AS DECIMAL(20, 10))", "Cannot cast VARCHAR '12345678901' to DECIMAL(20, 10)");
    }

    @Test
    public void testDecimalToVarcharCasts()
    {
        assertFunction("CAST(DECIMAL '2.34' AS VARCHAR)", VARCHAR, "2.34");
        assertFunction("CAST(DECIMAL '23400' AS VARCHAR)", VARCHAR, "23400");
        assertFunction("CAST(DECIMAL '0.0034' AS VARCHAR)", VARCHAR, "0.0034");
        assertFunction("CAST(DECIMAL '0' AS VARCHAR)", VARCHAR, "0");
        assertFunction("CAST(DECIMAL '0.1234567890123456' AS VARCHAR)", VARCHAR, "0.1234567890123456");

        assertFunction("CAST(DECIMAL '-10' AS VARCHAR)", VARCHAR, "-10");
        assertFunction("CAST(DECIMAL '-1.0' AS VARCHAR)", VARCHAR, "-1.0");
        assertFunction("CAST(DECIMAL '-1.00' AS VARCHAR)", VARCHAR, "-1.00");
        assertFunction("CAST(DECIMAL '-1.00000' AS VARCHAR)", VARCHAR, "-1.00000");
        assertFunction("CAST(DECIMAL '-0.1' AS VARCHAR)", VARCHAR, "-0.1");
        assertFunction("CAST(DECIMAL '-.001' AS VARCHAR)", VARCHAR, "-0.001");
        assertFunction("CAST(DECIMAL '-1234567890.1234567' AS VARCHAR)", VARCHAR, "-1234567890.1234567");

        assertFunction("CAST(DECIMAL '1234567890.1234567890' AS VARCHAR)", VARCHAR, "1234567890.1234567890");
        assertFunction("CAST(DECIMAL '-1234567890.1234567890' AS VARCHAR)", VARCHAR, "-1234567890.1234567890");
    }
}
