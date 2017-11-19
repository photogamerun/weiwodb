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

import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static java.lang.String.format;

import java.text.SimpleDateFormat;

import com.facebook.presto.operator.scalar.annotations.ScalarOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;

import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

public final class VarcharOperators {
	private VarcharOperators() {
	}

	@LiteralParameters({"x", "y"})
	@ScalarOperator(EQUAL)
	@SqlType(StandardTypes.BOOLEAN)
	public static boolean equal(@SqlType("varchar(x)") Slice left,
			@SqlType("varchar(y)") Slice right) {
		return left.equals(right);
	}

	@LiteralParameters({"x", "y"})
	@ScalarOperator(NOT_EQUAL)
	@SqlType(StandardTypes.BOOLEAN)
	public static boolean notEqual(@SqlType("varchar(x)") Slice left,
			@SqlType("varchar(y)") Slice right) {
		return !left.equals(right);
	}

	@LiteralParameters({"x", "y"})
	@ScalarOperator(LESS_THAN)
	@SqlType(StandardTypes.BOOLEAN)
	public static boolean lessThan(@SqlType("varchar(x)") Slice left,
			@SqlType("varchar(y)") Slice right) {
		return left.compareTo(right) < 0;
	}

	@LiteralParameters({"x", "y"})
	@ScalarOperator(LESS_THAN_OR_EQUAL)
	@SqlType(StandardTypes.BOOLEAN)
	public static boolean lessThanOrEqual(@SqlType("varchar(x)") Slice left,
			@SqlType("varchar(y)") Slice right) {
		return left.compareTo(right) <= 0;
	}

	@LiteralParameters({"x", "y"})
	@ScalarOperator(GREATER_THAN)
	@SqlType(StandardTypes.BOOLEAN)
	public static boolean greaterThan(@SqlType("varchar(x)") Slice left,
			@SqlType("varchar(y)") Slice right) {
		return left.compareTo(right) > 0;
	}

	@LiteralParameters({"x", "y"})
	@ScalarOperator(GREATER_THAN_OR_EQUAL)
	@SqlType(StandardTypes.BOOLEAN)
	public static boolean greaterThanOrEqual(@SqlType("varchar(x)") Slice left,
			@SqlType("varchar(y)") Slice right) {
		return left.compareTo(right) >= 0;
	}

	@LiteralParameters({"x", "y", "z"})
	@ScalarOperator(BETWEEN)
	@SqlType(StandardTypes.BOOLEAN)
	public static boolean between(@SqlType("varchar(x)") Slice value,
			@SqlType("varchar(y)") Slice min,
			@SqlType("varchar(z)") Slice max) {
		return min.compareTo(value) <= 0 && value.compareTo(max) <= 0;
	}

	@LiteralParameters("x")
	@ScalarOperator(CAST)
	@SqlType(StandardTypes.BOOLEAN)
	public static boolean castToBoolean(@SqlType("varchar(x)") Slice value) {
		if (value.length() == 1) {
			byte character = toUpperCase(value.getByte(0));
			if (character == 'T' || character == '1') {
				return true;
			}
			if (character == 'F' || character == '0') {
				return false;
			}
		}
		if ((value.length() == 4) && (toUpperCase(value.getByte(0)) == 'T')
				&& (toUpperCase(value.getByte(1)) == 'R')
				&& (toUpperCase(value.getByte(2)) == 'U')
				&& (toUpperCase(value.getByte(3)) == 'E')) {
			return true;
		}
		if ((value.length() == 5) && (toUpperCase(value.getByte(0)) == 'F')
				&& (toUpperCase(value.getByte(1)) == 'A')
				&& (toUpperCase(value.getByte(2)) == 'L')
				&& (toUpperCase(value.getByte(3)) == 'S')
				&& (toUpperCase(value.getByte(4)) == 'E')) {
			return false;
		}
		throw new PrestoException(INVALID_CAST_ARGUMENT,
				format("Cannot cast '%s' to BOOLEAN", value.toStringUtf8()));
	}

	private static byte toUpperCase(byte b) {
		return isLowerCase(b) ? ((byte) (b - 32)) : b;
	}

	private static boolean isLowerCase(byte b) {
		return (b >= 'a') && (b <= 'z');
	}

	@LiteralParameters("x")
	@ScalarOperator(CAST)
	@SqlType(StandardTypes.DOUBLE)
	public static double castToDouble(@SqlType("varchar(x)") Slice slice) {
		try {
			return Double.parseDouble(slice.toStringUtf8());
		} catch (Exception e) {
			throw new PrestoException(INVALID_CAST_ARGUMENT, format(
					"Can not cast '%s' to DOUBLE", slice.toStringUtf8()));
		}
	}

	@LiteralParameters("x")
	@ScalarOperator(CAST)
	@SqlType(StandardTypes.FLOAT)
	public static long castToFloat(@SqlType("varchar(x)") Slice slice) {
		try {
			return Float.floatToIntBits(Float.parseFloat(slice.toStringUtf8()));
		} catch (Exception e) {
			throw new PrestoException(INVALID_CAST_ARGUMENT,
					format("Can not cast '%s' to FLOAT", slice.toStringUtf8()));
		}
	}

	static SimpleDateFormat dataformat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	@LiteralParameters("x")
	@ScalarOperator(CAST)
	@SqlType(StandardTypes.BIGINT)
	public static long castToBigint(@SqlType("varchar(x)") Slice slice) { // TODO
		// try {
		// String value = slice.toStringUtf8();
		// Date data = dataformat.parse(String.valueOf(value));
		// return data.getTime();
		// } catch (Exception e) {
		// try {
		return Long.parseLong(slice.toStringUtf8());
		// } catch (Exception e1) {
		// throw new PrestoException(INVALID_CAST_ARGUMENT, format(
		// "Can not cast '%s' to BIGINT", slice.toStringUtf8()));
		// }
		// }
	}

	@LiteralParameters("x")
	@ScalarOperator(CAST)
	@SqlType(StandardTypes.INTEGER)
	public static long castToInteger(@SqlType("varchar(x)") Slice slice) {
		try {
			return Integer.parseInt(slice.toStringUtf8());
		} catch (Exception e) {
			throw new PrestoException(INVALID_CAST_ARGUMENT,
					format("Can not cast '%s' to INT", slice.toStringUtf8()));
		}
	}

	@LiteralParameters("x")
	@ScalarOperator(CAST)
	@SqlType(StandardTypes.SMALLINT)
	public static long castToSmallint(@SqlType("varchar(x)") Slice slice) {
		try {
			return Short.parseShort(slice.toStringUtf8());
		} catch (Exception e) {
			throw new PrestoException(INVALID_CAST_ARGUMENT, format(
					"Can not cast '%s' to SMALLINT", slice.toStringUtf8()));
		}
	}

	@LiteralParameters("x")
	@ScalarOperator(CAST)
	@SqlType(StandardTypes.TINYINT)
	public static long castToTinyint(@SqlType("varchar(x)") Slice slice) {
		try {
			return Byte.parseByte(slice.toStringUtf8());
		} catch (Exception e) {
			throw new PrestoException(INVALID_CAST_ARGUMENT, format(
					"Can not cast '%s' to TINYINT", slice.toStringUtf8()));
		}
	}

	@LiteralParameters("x")
	@ScalarOperator(CAST)
	@SqlType(StandardTypes.VARBINARY)
	public static Slice castToBinary(@SqlType("varchar(x)") Slice slice) {
		return slice;
	}

	@LiteralParameters("x")
	@ScalarOperator(HASH_CODE)
	@SqlType(StandardTypes.BIGINT)
	public static long hashCode(@SqlType("varchar(x)") Slice value) {
		return XxHash64.hash(value);
	}
}
