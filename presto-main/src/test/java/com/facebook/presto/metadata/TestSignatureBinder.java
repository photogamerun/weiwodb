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

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.longVariableExpression;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.metadata.Signature.withVariadicBound;
import static com.facebook.presto.metadata.SignatureBinder.bindVariables;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.testng.annotations.Test;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class TestSignatureBinder {
	private final TypeRegistry typeRegistry = new TypeRegistry();

	@Test
	public void testBindLiteralForDecimal() {
		TypeSignature leftType = parseTypeSignature("decimal(p1,s1)",
				ImmutableSet.of("p1", "s1"));
		TypeSignature rightType = parseTypeSignature("decimal(p2,s2)",
				ImmutableSet.of("p2", "s2"));

		Signature function = functionSignature()
				.returnType(parseTypeSignature(StandardTypes.BOOLEAN))
				.argumentTypes(leftType, rightType).build();

		assertThat(function).boundTo("decimal(2,1)", "decimal(1,0)")
				.produces(new BoundVariables(ImmutableMap.of(), ImmutableMap
						.of("p1", 2L, "s1", 1L, "p2", 1L, "s2", 0L)));
	}

	@Test
	public void testResolveCalculatedTypes() {
		TypeSignature leftType = parseTypeSignature("varchar(x)",
				ImmutableSet.of("x"));
		TypeSignature rightType = parseTypeSignature("varchar(y)",
				ImmutableSet.of("y"));

		Signature function = functionSignature()
				.returnType(parseTypeSignature(StandardTypes.BOOLEAN))
				.argumentTypes(leftType, rightType).build();

		assertThat(function).boundTo("varchar(42)", "varchar(44)")
				.produces(new BoundVariables(ImmutableMap.of(),
						ImmutableMap.of("x", 42L, "y", 44L)));

		assertThat(function).boundTo("unknown", "varchar(44)").withCoercion()
				.produces(new BoundVariables(ImmutableMap.of(),
						ImmutableMap.of("x", 0L, "y", 44L)));
	}

	@Test
	public void testBindUnknown() {
		Signature function = functionSignature()
				.returnType(parseTypeSignature(StandardTypes.BOOLEAN))
				.argumentTypes(
						parseTypeSignature("varchar(x)", ImmutableSet.of("x")))
				.build();

		assertThat(function).boundTo("unknown").fails();

		assertThat(function).boundTo("unknown").withCoercion().succeeds();
	}

	@Test
	public void testBindMixedLiteralAndTypeVariables() throws Exception {
		Signature function = functionSignature()
				.returnType(parseTypeSignature(StandardTypes.BOOLEAN))
				.typeVariableConstraints(ImmutableList.of(typeVariable("T")))
				.argumentTypes(parseTypeSignature("array(T)"),
						parseTypeSignature("array(decimal(p,s))",
								ImmutableSet.of("p", "s")))
				.build();

		assertThat(function)
				.boundTo("array(decimal(2,1))", "array(decimal(3,1))")
				.withCoercion()
				.produces(new BoundVariables(
						ImmutableMap.of("T", type("decimal(2,1)")),
						ImmutableMap.of("p", 3L, "s", 1L)));
	}

	@Test
	public void testBindDifferentLiteralParameters() throws Exception {
		TypeSignature argType = parseTypeSignature("decimal(p,s)",
				ImmutableSet.of("p", "s"));

		Signature function = functionSignature()
				.returnType(parseTypeSignature(StandardTypes.BOOLEAN))
				.argumentTypes(argType, argType).build();

		assertThat(function).boundTo("decimal(2,1)", "decimal(3,1)").fails();
	}

	@Test
	public void testBindCalculatedLiteralParameter() throws Exception {
		Set<String> literalParameters = ImmutableSet.of("p1", "p2", "p3", "s");
		TypeSignature leftType = parseTypeSignature("decimal(p1,s)",
				literalParameters);
		TypeSignature rightType = parseTypeSignature("decimal(p2,s)",
				literalParameters);
		TypeSignature returnType = parseTypeSignature("decimal(p3,s)",
				literalParameters);

		Signature function = functionSignature().returnType(returnType)
				.argumentTypes(leftType, rightType).longVariableConstraints(
						longVariableExpression("p3", "p1 + p2"))
				.build();

		assertThat(function).boundTo("decimal(2,1)", "decimal(3,1)")
				.produces(new BoundVariables(ImmutableMap.of(), ImmutableMap
						.of("p1", 2L, "p2", 3L, "p3", 5L, "s", 1L)));
	}

	@Test
	public void testBindUnknownToDecimal() throws Exception {
		Signature function = functionSignature()
				.returnType(parseTypeSignature("boolean"))
				.argumentTypes(parseTypeSignature("decimal(p,s)",
						ImmutableSet.of("p", "s")))
				.build();

		assertThat(function).boundTo("unknown").withCoercion()
				.produces(new BoundVariables(ImmutableMap.of(),
						ImmutableMap.of("p", 1L, "s", 0L)));
	}

	@Test
	public void testBindUnknownToConcreteArray() {
		Signature function = functionSignature()
				.returnType(parseTypeSignature(StandardTypes.BOOLEAN))
				.argumentTypes(parseTypeSignature("array(boolean)")).build();

		assertThat(function).boundTo("unknown").withCoercion().succeeds();
	}

	@Test
	public void testBindTypeVariablesBasedOnTheSecondArgument() {
		Signature function = functionSignature()
				.returnType(parseTypeSignature("T"))
				.argumentTypes(parseTypeSignature("array(T)"),
						parseTypeSignature("T"))
				.typeVariableConstraints(ImmutableList.of(typeVariable("T")))
				.build();

		assertThat(function).boundTo("unknown", "decimal(2,1)").withCoercion()
				.produces(new BoundVariables(
						ImmutableMap.of("T", type("decimal(2,1)")),
						ImmutableMap.of()));
	}

	@Test
	public void testBindParametricTypeParameterToUnknown() {
		Signature function = functionSignature()
				.returnType(parseTypeSignature("T"))
				.argumentTypes(parseTypeSignature("array(T)"))
				.typeVariableConstraints(ImmutableList.of(typeVariable("T")))
				.build();

		assertThat(function).boundTo("unknown").fails();

		assertThat(function).withCoercion().boundTo("unknown").fails();
	}

	@Test
	public void testBindUnknownToTypeParameter() {
		Signature function = functionSignature()
				.returnType(parseTypeSignature("T"))
				.argumentTypes(parseTypeSignature("T"))
				.typeVariableConstraints(ImmutableList.of(typeVariable("T")))
				.build();

		assertThat(function).boundTo("unknown").withCoercion()
				.produces(new BoundVariables(
						ImmutableMap.of("T", type("unknown")),
						ImmutableMap.of()));
	}

	@Test
	public void testBindDoubleToBigint() throws Exception {
		Signature function = functionSignature()
				.returnType(parseTypeSignature(StandardTypes.BOOLEAN))
				.argumentTypes(parseTypeSignature(StandardTypes.DOUBLE),
						parseTypeSignature(StandardTypes.DOUBLE))
				.build();

		assertThat(function).boundTo("double", "bigint").withCoercion()
				.succeeds();
	}

	@Test
	public void testBindVarcharTemplateStyle() {
		Signature function = functionSignature()
				.returnType(parseTypeSignature("T2"))
				.argumentTypes(parseTypeSignature("T1"))
				.typeVariableConstraints(ImmutableList.of(
						new TypeVariableConstraint("T1", true, false,
								"varchar"),
						new TypeVariableConstraint("T2", true, false,
								"varchar")))
				.build();

		assertThat(function)
				.boundTo(ImmutableList.of("varchar(42)"), "varchar(1)")
				.produces(
						new BoundVariables(
								ImmutableMap.of("T1", type("varchar(42)"), "T2",
										type("varchar(1)")),
								ImmutableMap.of()));
	}

	@Test
	public void testBindVarchar() {
		Signature function = functionSignature()
				.returnType(parseTypeSignature("varchar(42)"))
				.argumentTypes(parseTypeSignature("varchar(42)")).build();

		assertThat(function)
				.boundTo(ImmutableList.of("varchar(1)"), "varchar(1)").fails();

		assertThat(function)
				.boundTo(ImmutableList.of("varchar(1)"), "varchar(1)")
				.withCoercion().succeeds();

		assertThat(function)
				.boundTo(ImmutableList.of("varchar(44)"), "varchar(44)")
				.withCoercion().fails();
	}

	@Test
	public void testBindUnparametrizedVarchar() throws Exception {
		Signature function = functionSignature()
				.returnType(parseTypeSignature("boolean"))
				.argumentTypes(
						parseTypeSignature("varchar(x)", ImmutableSet.of("x")))
				.build();

		assertThat(function).boundTo("varchar")
				.produces(new BoundVariables(ImmutableMap.of(),
						ImmutableMap.of("x", (long) Integer.MAX_VALUE)));
	}

	@Test
	public void testBindToUnparametrizedVarchar() throws Exception {
		Signature function = functionSignature()
				.returnType(parseTypeSignature("boolean"))
				.argumentTypes(parseTypeSignature("varchar")).build();

		assertThat(function).boundTo("varchar(3)").succeeds();

		assertThat(function).boundTo("unknown").withCoercion().succeeds();
	}

	@Test
	public void testBasic() throws Exception {
		Signature function = functionSignature()
				.typeVariableConstraints(ImmutableList.of(typeVariable("T")))
				.returnType(parseTypeSignature("T"))
				.argumentTypes(parseTypeSignature("T")).build();

		assertThat(function).boundTo("bigint").produces(new BoundVariables(
				ImmutableMap.of("T", type("bigint")), ImmutableMap.of()));

		assertThat(function).boundTo("varchar").produces(new BoundVariables(
				ImmutableMap.of("T", type("varchar")), ImmutableMap.of()));

		assertThat(function).boundTo("varchar", "bigint").fails();

		assertThat(function).boundTo("array(bigint)")
				.produces(new BoundVariables(
						ImmutableMap.of("T", type("array(bigint)")),
						ImmutableMap.of()));
	}

	@Test
	public void testNonParametric() throws Exception {
		Signature function = functionSignature()
				.returnType(parseTypeSignature(StandardTypes.BOOLEAN))
				.argumentTypes(parseTypeSignature(StandardTypes.BIGINT))
				.build();

		assertThat(function).boundTo("bigint").succeeds();

		assertThat(function).boundTo("varchar").withCoercion().fails();

		assertThat(function).boundTo("varchar", "bigint").withCoercion()
				.fails();

		assertThat(function).boundTo("array(bigint)").withCoercion().fails();
	}

	public static void main(String[] args) {
		System.out.println(parseTypeSignature("array(HyperLogLog)"));
	}

	@Test
	public void testArray() throws Exception {
		Signature getFunction = functionSignature()
				.returnType(parseTypeSignature("T"))
				.argumentTypes(parseTypeSignature("array(T)"))
				.typeVariableConstraints(ImmutableList.of(typeVariable("T")))
				.build();

		assertThat(getFunction).boundTo("array(bigint)").produces(
				new BoundVariables(ImmutableMap.of("T", type("bigint")),
						ImmutableMap.of()));

		assertThat(getFunction).boundTo("bigint").withCoercion().fails();

		Signature containsFunction = functionSignature()
				.returnType(parseTypeSignature("T"))
				.argumentTypes(parseTypeSignature("array(T)"),
						parseTypeSignature("T"))
				.typeVariableConstraints(
						ImmutableList.of(comparableTypeParameter("T")))
				.build();

		assertThat(containsFunction).boundTo("array(bigint)", "bigint")
				.produces(
						new BoundVariables(ImmutableMap.of("T", type("bigint")),
								ImmutableMap.of()));

		assertThat(containsFunction).boundTo("array(bigint)", "varchar")
				.withCoercion().fails();

		assertThat(containsFunction)
				.boundTo("array(HyperLogLog)", "HyperLogLog").withCoercion()
				.fails();

		Signature castFunction = functionSignature()
				.returnType(parseTypeSignature("array(T2)"))
				.argumentTypes(parseTypeSignature("array(T1)"),
						parseTypeSignature("array(T2)"))
				.typeVariableConstraints(ImmutableList.of(typeVariable("T1"),
						typeVariable("T2")))
				.build();

		assertThat(castFunction)
				.boundTo("array(unknown)", "array(decimal(2,1))").withCoercion()
				.produces(
						new BoundVariables(
								ImmutableMap.of("T1", type("unknown"), "T2",
										type("decimal(2,1)")),
								ImmutableMap.of()));

		Signature fooFunction = functionSignature()
				.returnType(parseTypeSignature("T"))
				.argumentTypes(parseTypeSignature("array(T)"),
						parseTypeSignature("array(T)"))
				.typeVariableConstraints(ImmutableList.of(typeVariable("T")))
				.build();

		assertThat(fooFunction).boundTo("array(bigint)", "array(bigint)")
				.produces(
						new BoundVariables(ImmutableMap.of("T", type("bigint")),
								ImmutableMap.of()));

		assertThat(fooFunction).boundTo("array(bigint)", "array(varchar)")
				.withCoercion().fails();
	}

	@Test
	public void testMap() throws Exception {
		Signature getValueFunction = functionSignature()
				.returnType(parseTypeSignature("V"))
				.argumentTypes(parseTypeSignature("map(K,V)"),
						parseTypeSignature("K"))
				.typeVariableConstraints(
						ImmutableList.of(typeVariable("K"), typeVariable("V")))
				.build();

		assertThat(getValueFunction).boundTo("map(bigint,varchar)", "bigint")
				.produces(
						new BoundVariables(ImmutableMap.of("K", type("bigint"),
								"V", type("varchar")), ImmutableMap.of()));

		assertThat(getValueFunction).boundTo("map(bigint,varchar)", "varchar")
				.withCoercion().fails();
	}

	@Test
	public void testVariadic() throws Exception {
		Signature mapVariadicBoundFunction = functionSignature()
				.returnType(parseTypeSignature(StandardTypes.BIGINT))
				.argumentTypes(parseTypeSignature("T"))
				.typeVariableConstraints(
						ImmutableList.of(withVariadicBound("T", "map")))
				.build();

		assertThat(mapVariadicBoundFunction).boundTo("map(bigint,bigint)")
				.produces(new BoundVariables(
						ImmutableMap.of("T", type("map(bigint,bigint)")),
						ImmutableMap.of()));

		assertThat(mapVariadicBoundFunction).boundTo("array(bigint)").fails();

		assertThat(mapVariadicBoundFunction).boundTo("array(bigint)")
				.withCoercion().fails();

		Signature decimalVariadicBoundFunction = functionSignature()
				.returnType(parseTypeSignature("bigint"))
				.argumentTypes(parseTypeSignature("T"))
				.typeVariableConstraints(
						ImmutableList.of(withVariadicBound("T", "decimal")))
				.build();

		assertThat(decimalVariadicBoundFunction).boundTo("decimal(2,1)")
				.produces(new BoundVariables(
						ImmutableMap.of("T", type("decimal(2,1)")),
						ImmutableMap.of()));

		assertThat(decimalVariadicBoundFunction).boundTo("bigint").fails();
	}

	@Test
	public void testBindUnknownToVariadic() {
		Signature rowFunction = functionSignature()
				.returnType(parseTypeSignature(StandardTypes.BOOLEAN))
				.argumentTypes(parseTypeSignature("T"), parseTypeSignature("T"))
				.typeVariableConstraints(
						ImmutableList.of(withVariadicBound("T", "row")))
				.build();

		assertThat(rowFunction).boundTo("unknown", "row(a bigint)")
				.withCoercion()
				.produces(new BoundVariables(
						ImmutableMap.of("T", type("row(a bigint)")),
						ImmutableMap.of()));

		Signature arrayFunction = functionSignature()
				.returnType(parseTypeSignature(StandardTypes.BOOLEAN))
				.argumentTypes(parseTypeSignature("T"), parseTypeSignature("T"))
				.typeVariableConstraints(
						ImmutableList.of(withVariadicBound("T", "array")))
				.build();

		assertThat(arrayFunction).boundTo("unknown", "array(bigint)")
				.withCoercion()
				.produces(new BoundVariables(
						ImmutableMap.of("T", type("array(bigint)")),
						ImmutableMap.of()));
	}

	@Test
	public void testVarArgs() throws Exception {
		Signature variableArityFunction = functionSignature()
				.returnType(parseTypeSignature(StandardTypes.BOOLEAN))
				.argumentTypes(parseTypeSignature("T"))
				.typeVariableConstraints(ImmutableList.of(typeVariable("T")))
				.setVariableArity(true).build();

		assertThat(variableArityFunction).boundTo("bigint").produces(
				new BoundVariables(ImmutableMap.of("T", type("bigint")),
						ImmutableMap.of()));

		assertThat(variableArityFunction).boundTo("varchar")
				.produces(new BoundVariables(
						ImmutableMap.of("T", type("varchar")),
						ImmutableMap.of()));

		assertThat(variableArityFunction).boundTo("bigint", "bigint").produces(
				new BoundVariables(ImmutableMap.of("T", type("bigint")),
						ImmutableMap.of()));

		assertThat(variableArityFunction).boundTo("bigint", "varchar")
				.withCoercion().fails();
	}

	@Test
	public void testCoercion() throws Exception {
		Signature function = functionSignature()
				.returnType(parseTypeSignature(StandardTypes.BOOLEAN))
				.argumentTypes(parseTypeSignature("T"),
						parseTypeSignature(StandardTypes.DOUBLE))
				.typeVariableConstraints(ImmutableList.of(typeVariable("T")))
				.build();

		assertThat(function).boundTo("double", "double").withCoercion()
				.produces(
						new BoundVariables(ImmutableMap.of("T", type("double")),
								ImmutableMap.of()));

		assertThat(function).boundTo("bigint", "bigint").withCoercion()
				.produces(
						new BoundVariables(ImmutableMap.of("T", type("bigint")),
								ImmutableMap.of()));

		assertThat(function).boundTo("varchar", "bigint").withCoercion()
				.produces(new BoundVariables(
						ImmutableMap.of("T", type("varchar")),
						ImmutableMap.of()));

		assertThat(function).boundTo("bigint", "varchar").withCoercion()
				.fails();
	}

	@Test
	public void testUnknownCoercion() throws Exception {
		Signature foo = functionSignature()
				.returnType(parseTypeSignature("boolean"))
				.argumentTypes(parseTypeSignature("T"), parseTypeSignature("T"))
				.typeVariableConstraints(ImmutableList.of(typeVariable("T")))
				.build();

		assertThat(foo).boundTo("unknown", "unknown")
				.produces(new BoundVariables(
						ImmutableMap.of("T", type("unknown")),
						ImmutableMap.of()));

		assertThat(foo).boundTo("unknown", "bigint").withCoercion().produces(
				new BoundVariables(ImmutableMap.of("T", type("bigint")),
						ImmutableMap.of()));

		assertThat(foo).boundTo("varchar", "bigint").withCoercion().fails();

		Signature bar = functionSignature()
				.returnType(parseTypeSignature("boolean"))
				.argumentTypes(parseTypeSignature("T"), parseTypeSignature("T"))
				.typeVariableConstraints(
						ImmutableList.of(comparableTypeParameter("T")))
				.build();

		assertThat(bar).boundTo("unknown", "bigint").withCoercion().produces(
				new BoundVariables(ImmutableMap.of("T", type("bigint")),
						ImmutableMap.of()));

		assertThat(bar).boundTo("varchar", "bigint").withCoercion().fails();

		assertThat(bar).boundTo("HyperLogLog", "HyperLogLog").withCoercion()
				.fails();
	}

	@Test
	public void testBindParameters() throws Exception {
		BoundVariables boundVariables = BoundVariables.builder()
				.setTypeVariable("T1", DoubleType.DOUBLE)
				.setTypeVariable("T2", BigintType.BIGINT)
				.setTypeVariable("T3", DecimalType.createDecimalType(5, 3))
				.setLongVariable("p", 1L).setLongVariable("s", 2L).build();

		assertThat("bigint", boundVariables, "bigint");
		assertThat("T1", boundVariables, "double");
		assertThat("T2", boundVariables, "bigint");
		assertThat("array(T1)", boundVariables, "array(double)");
		assertThat("array(T3)", boundVariables, "array(decimal(5,3))");
		assertThat("array<T1>", boundVariables, "array(double)");
		assertThat("map(T1,T2)", boundVariables, "map(double,bigint)");
		assertThat("map<T1,T2>", boundVariables, "map(double,bigint)");
		assertThat("bla(T1,42,T2)", boundVariables, "bla(double,42,bigint)");
		assertThat("varchar(p)", boundVariables, "varchar(1)");
		assertThat("decimal(p,s)", boundVariables, "decimal(1,2)");
		assertThat("array(decimal(p,s))", boundVariables,
				"array(decimal(1,2))");

		assertBindVariablesFails("T1(bigint)", boundVariables,
				"Unbounded parameters can not have parameters");
	}

	private void assertBindVariablesFails(String typeSignature,
			BoundVariables boundVariables, String reason) {
		try {
			bindVariables(parseTypeSignature(typeSignature,
					ImmutableSet.of("p", "s")), boundVariables);
			fail(reason);
		} catch (RuntimeException e) {
			// Expected
		}
	}

	private void assertThat(String typeSignature, BoundVariables boundVariables,
			String expectedTypeSignature) {
		assertEquals(
				bindVariables(parseTypeSignature(typeSignature,
						ImmutableSet.of("p", "s")), boundVariables).toString(),
				expectedTypeSignature);
	}

	private SignatureBuilder functionSignature() {
		return new SignatureBuilder().name("function").kind(SCALAR);
	}

	private Type type(String signature) {
		TypeSignature typeSignature = TypeSignature
				.parseTypeSignature(signature);
		return typeRegistry.getType(typeSignature);
	}

	private List<Type> types(String... signatures) {
		return ImmutableList.copyOf(signatures).stream().map(this::type)
				.collect(toList());
	}

	private BindSignatureAssertion assertThat(Signature function) {
		return new BindSignatureAssertion(function);
	}

	private class BindSignatureAssertion {
		private final Signature function;
		private List<Type> argumentTypes = null;
		private Type returnType = null;
		private boolean allowCoercion = false;

		private BindSignatureAssertion(Signature function) {
			this.function = function;
		}

		public BindSignatureAssertion withCoercion() {
			allowCoercion = true;
			return this;
		}

		public BindSignatureAssertion boundTo(String... arguments) {
			this.argumentTypes = types(arguments);
			return this;
		}

		public BindSignatureAssertion boundTo(List<String> arguments,
				String returnType) {
			this.argumentTypes = types(
					arguments.toArray(new String[arguments.size()]));
			this.returnType = type(returnType);
			return this;
		}

		public BindSignatureAssertion succeeds() {
			assertTrue(bindVariables().isPresent());
			return this;
		}

		public BindSignatureAssertion fails() {
			assertFalse(bindVariables().isPresent());
			return this;
		}

		public BindSignatureAssertion produces(BoundVariables expected) {
			Optional<BoundVariables> actual = bindVariables();
			assertTrue(actual.isPresent());
			assertEquals(actual.get(), expected);
			return this;
		}

		private Optional<BoundVariables> bindVariables() {
			assertNotNull(argumentTypes);
			SignatureBinder signatureBinder = new SignatureBinder(typeRegistry,
					function, allowCoercion);
			if (returnType == null) {
				return signatureBinder.bindVariables(argumentTypes);
			} else {
				return signatureBinder.bindVariables(argumentTypes, returnType);
			}
		}
	}
}
