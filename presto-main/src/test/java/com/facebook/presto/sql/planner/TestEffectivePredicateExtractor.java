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
package com.facebook.presto.sql.planner;

import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.or;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.type.UnknownType;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

@Test(singleThreaded = true)
public class TestEffectivePredicateExtractor {
	private static final TableHandle DUAL_TABLE_HANDLE = new TableHandle("test",
			new TestingTableHandle());

	private static final Symbol A = new Symbol("a");
	private static final Symbol B = new Symbol("b");
	private static final Symbol C = new Symbol("c");
	private static final Symbol D = new Symbol("d");
	private static final Symbol E = new Symbol("e");
	private static final Symbol F = new Symbol("f");
	private static final Expression AE = A.toSymbolReference();
	private static final Expression BE = B.toSymbolReference();
	private static final Expression CE = C.toSymbolReference();
	private static final Expression DE = D.toSymbolReference();
	private static final Expression EE = E.toSymbolReference();
	private static final Expression FE = F.toSymbolReference();

	private static final Map<Symbol, Type> TYPES = ImmutableMap
			.<Symbol, Type> builder().put(A, BIGINT).put(B, BIGINT)
			.put(C, BIGINT).put(D, BIGINT).put(E, BIGINT).put(F, BIGINT)
			.build();

	private Map<Symbol, ColumnHandle> scanAssignments;
	private TableScanNode baseTableScan;
	private ExpressionIdentityNormalizer expressionNormalizer;

	@BeforeMethod
	public void setUp() throws Exception {
		scanAssignments = ImmutableMap.<Symbol, ColumnHandle> builder()
				.put(A, new TestingColumnHandle("a"))
				.put(B, new TestingColumnHandle("b"))
				.put(C, new TestingColumnHandle("c"))
				.put(D, new TestingColumnHandle("d"))
				.put(E, new TestingColumnHandle("e"))
				.put(F, new TestingColumnHandle("f")).build();

		Map<Symbol, ColumnHandle> assignments = Maps.filterKeys(scanAssignments,
				Predicates.in(ImmutableList.of(A, B, C, D, E, F)));
		baseTableScan = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(assignments.keySet()), assignments,
				Optional.empty(), TupleDomain.all(), null);

		expressionNormalizer = new ExpressionIdentityNormalizer();
	}

	@Test
	public void testAggregation() throws Exception {
		PlanNode node = new AggregationNode(newId(), filter(baseTableScan,
				and(equals(AE, DE), equals(BE, EE), equals(CE, FE),
						lessThan(DE, bigintLiteral(10)), lessThan(CE, DE),
						greaterThan(AE, bigintLiteral(2)), equals(EE, FE))),
				ImmutableList.of(A, B, C),
				ImmutableMap.of(C, fakeFunction("test"), D,
						fakeFunction("test")),
				ImmutableMap.of(C, fakeFunctionHandle("test", AGGREGATE), D,
						fakeFunctionHandle("test", AGGREGATE)),
				ImmutableMap.<Symbol, Symbol> of(),
				ImmutableList.of(ImmutableList.of(A, B, C)),
				AggregationNode.Step.FINAL, Optional.empty(), 1.0,
				Optional.empty(), null);

		Expression effectivePredicate = EffectivePredicateExtractor
				.extract(node, TYPES);

		// Rewrite in terms of group by symbols
		assertEquals(normalizeConjuncts(effectivePredicate),
				normalizeConjuncts(lessThan(AE, bigintLiteral(10)),
						lessThan(BE, AE), greaterThan(AE, bigintLiteral(2)),
						equals(BE, CE)));
	}

	@Test
	public void testFilter() throws Exception {
		PlanNode node = filter(baseTableScan, and(
				greaterThan(AE,
						new FunctionCall(QualifiedName.of("rand"),
								ImmutableList.<Expression> of())),
				lessThan(BE, bigintLiteral(10))));

		Expression effectivePredicate = EffectivePredicateExtractor
				.extract(node, TYPES);

		// Non-deterministic functions should be purged
		assertEquals(normalizeConjuncts(effectivePredicate),
				normalizeConjuncts(lessThan(BE, bigintLiteral(10))));
	}

	@Test
	public void testProject() throws Exception {
		PlanNode node = new ProjectNode(newId(),
				filter(baseTableScan,
						and(equals(AE, BE), equals(BE, CE),
								lessThan(CE, bigintLiteral(10)))),
				ImmutableMap.of(D, AE, E, CE));

		Expression effectivePredicate = EffectivePredicateExtractor
				.extract(node, TYPES);

		// Rewrite in terms of project output symbols
		assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjuncts(
				lessThan(DE, bigintLiteral(10)), equals(DE, EE)));
	}

	@Test
	public void testTopN() throws Exception {
		PlanNode node = new TopNNode(newId(),
				filter(baseTableScan,
						and(equals(AE, BE), equals(BE, CE),
								lessThan(CE, bigintLiteral(10)))),
				1, ImmutableList.of(A),
				ImmutableMap.of(A, SortOrder.ASC_NULLS_LAST), true);

		Expression effectivePredicate = EffectivePredicateExtractor
				.extract(node, TYPES);

		// Pass through
		assertEquals(normalizeConjuncts(effectivePredicate),
				normalizeConjuncts(equals(AE, BE), equals(BE, CE),
						lessThan(CE, bigintLiteral(10))));
	}

	@Test
	public void testLimit() throws Exception {
		PlanNode node = new LimitNode(
				newId(), filter(baseTableScan, and(equals(AE, BE),
						equals(BE, CE), lessThan(CE, bigintLiteral(10)))),
				1, false);

		Expression effectivePredicate = EffectivePredicateExtractor
				.extract(node, TYPES);

		// Pass through
		assertEquals(normalizeConjuncts(effectivePredicate),
				normalizeConjuncts(equals(AE, BE), equals(BE, CE),
						lessThan(CE, bigintLiteral(10))));
	}

	@Test
	public void testSort() throws Exception {
		PlanNode node = new SortNode(newId(),
				filter(baseTableScan,
						and(equals(AE, BE), equals(BE, CE),
								lessThan(CE, bigintLiteral(10)))),
				ImmutableList.of(A),
				ImmutableMap.of(A, SortOrder.ASC_NULLS_LAST));

		Expression effectivePredicate = EffectivePredicateExtractor
				.extract(node, TYPES);

		// Pass through
		assertEquals(normalizeConjuncts(effectivePredicate),
				normalizeConjuncts(equals(AE, BE), equals(BE, CE),
						lessThan(CE, bigintLiteral(10))));
	}

	@Test
	public void testWindow() throws Exception {
		PlanNode node = new WindowNode(newId(),
				filter(baseTableScan,
						and(equals(AE, BE), equals(BE, CE),
								lessThan(CE, bigintLiteral(10)))),
				new WindowNode.Specification(ImmutableList.of(A),
						ImmutableList.of(A),
						ImmutableMap.of(A, SortOrder.ASC_NULLS_LAST),
						new WindowNode.Frame(WindowFrame.Type.RANGE,
								FrameBound.Type.UNBOUNDED_PRECEDING,
								Optional.empty(), FrameBound.Type.CURRENT_ROW,
								Optional.empty())),
				ImmutableMap.<Symbol, FunctionCall> of(),
				ImmutableMap.<Symbol, Signature> of(), Optional.empty(),
				ImmutableSet.of(), 0);

		Expression effectivePredicate = EffectivePredicateExtractor
				.extract(node, TYPES);

		// Pass through
		assertEquals(normalizeConjuncts(effectivePredicate),
				normalizeConjuncts(equals(AE, BE), equals(BE, CE),
						lessThan(CE, bigintLiteral(10))));
	}

	@Test
	public void testTableScan() throws Exception {
		// Effective predicate is True if there is no effective predicate
		Map<Symbol, ColumnHandle> assignments = Maps.filterKeys(scanAssignments,
				Predicates.in(ImmutableList.of(A, B, C, D)));
		PlanNode node = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(assignments.keySet()), assignments,
				Optional.empty(), TupleDomain.all(), null);
		Expression effectivePredicate = EffectivePredicateExtractor
				.extract(node, TYPES);
		assertEquals(effectivePredicate, BooleanLiteral.TRUE_LITERAL);

		node = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(assignments.keySet()), assignments,
				Optional.empty(), TupleDomain.none(), null);
		effectivePredicate = EffectivePredicateExtractor.extract(node, TYPES);
		assertEquals(effectivePredicate, FALSE_LITERAL);

		node = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(assignments.keySet()), assignments,
				Optional.empty(),
				TupleDomain.withColumnDomains(
						ImmutableMap.of(scanAssignments.get(A),
								Domain.singleValue(BIGINT, 1L))),
				null);
		effectivePredicate = EffectivePredicateExtractor.extract(node, TYPES);
		assertEquals(normalizeConjuncts(effectivePredicate),
				normalizeConjuncts(equals(bigintLiteral(1L), AE)));

		node = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(assignments.keySet()), assignments,
				Optional.empty(),
				TupleDomain.withColumnDomains(ImmutableMap.of(
						scanAssignments.get(A), Domain.singleValue(BIGINT, 1L),
						scanAssignments.get(B),
						Domain.singleValue(BIGINT, 2L))),
				null);
		effectivePredicate = EffectivePredicateExtractor.extract(node, TYPES);
		assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjuncts(
				equals(bigintLiteral(2L), BE), equals(bigintLiteral(1L), AE)));

		node = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(assignments.keySet()), assignments,
				Optional.empty(), TupleDomain.all(), null);
		effectivePredicate = EffectivePredicateExtractor.extract(node, TYPES);
		assertEquals(effectivePredicate, BooleanLiteral.TRUE_LITERAL);
	}

	@Test
	public void testUnion() throws Exception {
		ImmutableListMultimap<Symbol, Symbol> symbolMapping = ImmutableListMultimap
				.of(A, B, A, C, A, E);
		PlanNode node = new UnionNode(newId(),
				ImmutableList.<PlanNode> of(
						filter(baseTableScan,
								greaterThan(AE,
										bigintLiteral(10))),
						filter(baseTableScan,
								and(greaterThan(AE, bigintLiteral(10)),
										lessThan(AE, bigintLiteral(100)))),
						filter(baseTableScan,
								and(greaterThan(AE, bigintLiteral(10)),
										lessThan(AE, bigintLiteral(100))))),
				symbolMapping, ImmutableList.copyOf(symbolMapping.keySet()));

		Expression effectivePredicate = EffectivePredicateExtractor
				.extract(node, TYPES);

		// Only the common conjuncts can be inferred through a Union
		assertEquals(normalizeConjuncts(effectivePredicate),
				normalizeConjuncts(greaterThan(AE, bigintLiteral(10))));
	}

	@Test
	public void testInnerJoin() throws Exception {
		ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList
				.builder();
		criteriaBuilder.add(new JoinNode.EquiJoinClause(A, D));
		criteriaBuilder.add(new JoinNode.EquiJoinClause(B, E));
		List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

		Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(
				scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
		TableScanNode leftScan = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(leftAssignments.keySet()), leftAssignments,
				Optional.empty(), TupleDomain.all(), null);

		Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(
				scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
		TableScanNode rightScan = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(rightAssignments.keySet()),
				rightAssignments, Optional.empty(), TupleDomain.all(), null);

		PlanNode node = new JoinNode(newId(), JoinNode.Type.INNER,
				filter(leftScan,
						and(lessThan(BE, AE), lessThan(CE, bigintLiteral(10)))),
				filter(rightScan,
						and(equals(DE, EE), lessThan(FE, bigintLiteral(100)))),
				criteria, Optional.empty(), Optional.empty(), Optional.empty());

		Expression effectivePredicate = EffectivePredicateExtractor
				.extract(node, TYPES);

		// All predicates should be carried through
		assertEquals(normalizeConjuncts(effectivePredicate),
				normalizeConjuncts(lessThan(BE, AE),
						lessThan(CE, bigintLiteral(10)), equals(DE, EE),
						lessThan(FE, bigintLiteral(100)), equals(AE, DE),
						equals(BE, EE)));
	}

	@Test
	public void testLeftJoin() throws Exception {
		ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList
				.builder();
		criteriaBuilder.add(new JoinNode.EquiJoinClause(A, D));
		criteriaBuilder.add(new JoinNode.EquiJoinClause(B, E));
		List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

		Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(
				scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
		TableScanNode leftScan = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(leftAssignments.keySet()), leftAssignments,
				Optional.empty(), TupleDomain.all(), null);

		Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(
				scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
		TableScanNode rightScan = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(rightAssignments.keySet()),
				rightAssignments, Optional.empty(), TupleDomain.all(), null);

		PlanNode node = new JoinNode(newId(), JoinNode.Type.LEFT,
				filter(leftScan,
						and(lessThan(BE, AE), lessThan(CE, bigintLiteral(10)))),
				filter(rightScan,
						and(equals(DE, EE), lessThan(FE, bigintLiteral(100)))),
				criteria, Optional.empty(), Optional.empty(), Optional.empty());

		Expression effectivePredicate = EffectivePredicateExtractor
				.extract(node, TYPES);

		// All right side symbols should be checked against NULL
		assertEquals(normalizeConjuncts(effectivePredicate),
				normalizeConjuncts(lessThan(BE, AE),
						lessThan(CE, bigintLiteral(10)),
						or(equals(DE, EE), and(isNull(DE), isNull(EE))),
						or(lessThan(FE, bigintLiteral(100)), isNull(FE)),
						or(equals(AE, DE), isNull(DE)),
						or(equals(BE, EE), isNull(EE))));
	}

	@Test
	public void testLeftJoinWithFalseInner() throws Exception {
		List<JoinNode.EquiJoinClause> criteria = ImmutableList
				.of(new JoinNode.EquiJoinClause(A, D));

		Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(
				scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
		TableScanNode leftScan = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(leftAssignments.keySet()), leftAssignments,
				Optional.empty(), TupleDomain.all(), null);

		Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(
				scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
		TableScanNode rightScan = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(rightAssignments.keySet()),
				rightAssignments, Optional.empty(), TupleDomain.all(), null);

		PlanNode node = new JoinNode(newId(), JoinNode.Type.LEFT,
				filter(leftScan,
						and(lessThan(BE, AE), lessThan(CE, bigintLiteral(10)))),
				filter(rightScan, FALSE_LITERAL), criteria, Optional.empty(),
				Optional.empty(), Optional.empty());

		Expression effectivePredicate = EffectivePredicateExtractor
				.extract(node, TYPES);

		// False literal on the right side should be ignored
		assertEquals(normalizeConjuncts(effectivePredicate),
				normalizeConjuncts(lessThan(BE, AE),
						lessThan(CE, bigintLiteral(10)),
						or(equals(AE, DE), isNull(DE))));
	}

	@Test
	public void testRightJoin() throws Exception {
		ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList
				.builder();
		criteriaBuilder.add(new JoinNode.EquiJoinClause(A, D));
		criteriaBuilder.add(new JoinNode.EquiJoinClause(B, E));
		List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

		Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(
				scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
		TableScanNode leftScan = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(leftAssignments.keySet()), leftAssignments,
				Optional.empty(), TupleDomain.all(), null);

		Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(
				scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
		TableScanNode rightScan = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(rightAssignments.keySet()),
				rightAssignments, Optional.empty(), TupleDomain.all(), null);

		PlanNode node = new JoinNode(newId(), JoinNode.Type.RIGHT,
				filter(leftScan,
						and(lessThan(BE, AE), lessThan(CE, bigintLiteral(10)))),
				filter(rightScan,
						and(equals(DE, EE), lessThan(FE, bigintLiteral(100)))),
				criteria, Optional.empty(), Optional.empty(), Optional.empty());

		Expression effectivePredicate = EffectivePredicateExtractor
				.extract(node, TYPES);

		// All left side symbols should be checked against NULL
		assertEquals(normalizeConjuncts(effectivePredicate),
				normalizeConjuncts(
						or(lessThan(BE, AE), and(isNull(BE), isNull(AE))),
						or(lessThan(CE, bigintLiteral(10)), isNull(CE)),
						equals(DE, EE), lessThan(FE, bigintLiteral(100)),
						or(equals(AE, DE), isNull(AE)),
						or(equals(BE, EE), isNull(BE))));
	}

	@Test
	public void testRightJoinWithFalseInner() throws Exception {
		List<JoinNode.EquiJoinClause> criteria = ImmutableList
				.of(new JoinNode.EquiJoinClause(A, D));

		Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(
				scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
		TableScanNode leftScan = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(leftAssignments.keySet()), leftAssignments,
				Optional.empty(), TupleDomain.all(), null);

		Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(
				scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
		TableScanNode rightScan = new TableScanNode(newId(), DUAL_TABLE_HANDLE,
				ImmutableList.copyOf(rightAssignments.keySet()),
				rightAssignments, Optional.empty(), TupleDomain.all(), null);

		PlanNode node = new JoinNode(newId(), JoinNode.Type.RIGHT,
				filter(leftScan, FALSE_LITERAL),
				filter(rightScan,
						and(equals(DE, EE), lessThan(FE, bigintLiteral(100)))),
				criteria, Optional.empty(), Optional.empty(), Optional.empty());

		Expression effectivePredicate = EffectivePredicateExtractor
				.extract(node, TYPES);

		// False literal on the left side should be ignored
		assertEquals(normalizeConjuncts(effectivePredicate),
				normalizeConjuncts(equals(DE, EE),
						lessThan(FE, bigintLiteral(100)),
						or(equals(AE, DE), isNull(AE))));
	}

	@Test
	public void testSemiJoin() throws Exception {
		PlanNode node = new SemiJoinNode(newId(),
				filter(baseTableScan,
						and(greaterThan(AE, bigintLiteral(10)),
								lessThan(AE, bigintLiteral(100)))),
				filter(baseTableScan, greaterThan(AE, bigintLiteral(5))), A, B,
				C, Optional.empty(), Optional.empty());

		Expression effectivePredicate = EffectivePredicateExtractor
				.extract(node, TYPES);

		// Currently, only pull predicates through the source plan
		assertEquals(normalizeConjuncts(effectivePredicate),
				normalizeConjuncts(and(greaterThan(AE, bigintLiteral(10)),
						lessThan(AE, bigintLiteral(100)))));
	}

	private static PlanNodeId newId() {
		return new PlanNodeId(UUID.randomUUID().toString());
	}

	private static FilterNode filter(PlanNode source, Expression predicate) {
		return new FilterNode(newId(), source, predicate);
	}

	private static Expression bigintLiteral(long number) {
		if (number < Integer.MAX_VALUE && number > Integer.MIN_VALUE) {
			return new GenericLiteral("BIGINT", String.valueOf(number));
		}
		return new LongLiteral(String.valueOf(number));
	}

	private static ComparisonExpression equals(Expression expression1,
			Expression expression2) {
		return new ComparisonExpression(ComparisonExpression.Type.EQUAL,
				expression1, expression2);
	}

	private static ComparisonExpression lessThan(Expression expression1,
			Expression expression2) {
		return new ComparisonExpression(ComparisonExpression.Type.LESS_THAN,
				expression1, expression2);
	}

	private static ComparisonExpression greaterThan(Expression expression1,
			Expression expression2) {
		return new ComparisonExpression(ComparisonExpression.Type.GREATER_THAN,
				expression1, expression2);
	}

	private static IsNullPredicate isNull(Expression expression) {
		return new IsNullPredicate(expression);
	}

	private static FunctionCall fakeFunction(String name) {
		return new FunctionCall(QualifiedName.of("test"),
				ImmutableList.<Expression> of());
	}

	private static Signature fakeFunctionHandle(String name,
			FunctionKind kind) {
		return new Signature(name, kind,
				TypeSignature.parseTypeSignature(UnknownType.NAME),
				ImmutableList.<TypeSignature> of());
	}

	private Set<Expression> normalizeConjuncts(Expression... conjuncts) {
		return normalizeConjuncts(Arrays.asList(conjuncts));
	}

	private Set<Expression> normalizeConjuncts(
			Collection<Expression> conjuncts) {
		return normalizeConjuncts(combineConjuncts(conjuncts));
	}

	private Set<Expression> normalizeConjuncts(Expression predicate) {
		// Normalize the predicate by identity so that the EqualityInference
		// will produce stable rewrites in this test
		// and thereby produce comparable Sets of conjuncts from this method.
		predicate = expressionNormalizer.normalize(predicate);

		// Equality inference rewrites and equality generation will always be
		// stable across multiple runs in the same JVM
		EqualityInference inference = EqualityInference
				.createEqualityInference(predicate);

		Set<Expression> rewrittenSet = new HashSet<>();
		for (Expression expression : EqualityInference
				.nonInferrableConjuncts(predicate)) {
			Expression rewritten = inference.rewriteExpression(expression,
					Predicates.<Symbol> alwaysTrue());
			Preconditions.checkState(rewritten != null,
					"Rewrite with full symbol scope should always be possible");
			rewrittenSet.add(rewritten);
		}
		rewrittenSet
				.addAll(inference
						.generateEqualitiesPartitionedBy(
								Predicates.<Symbol> alwaysTrue())
						.getScopeEqualities());

		return rewrittenSet;
	}

	/**
	 * Normalizes Expression nodes (and all sub-expressions) by identity.
	 * <p>
	 * Identity equality of Expression nodes is necessary for EqualityInference
	 * to generate stable rewrites (as specified by Ordering.arbitrary())
	 */
	private static class ExpressionIdentityNormalizer {
		private final Map<Expression, Expression> expressionCache = new HashMap<>();

		private Expression normalize(Expression expression) {
			Expression identityNormalizedExpression = expressionCache
					.get(expression);
			if (identityNormalizedExpression == null) {
				// Make sure all sub-expressions are normalized first
				for (Expression subExpression : Iterables.filter(
						SubExpressionExtractor.extract(expression),
						Predicates.not(Predicates.equalTo(expression)))) {
					normalize(subExpression);
				}

				// Since we have not seen this expression before, rewrite it
				// entirely in terms of the normalized sub-expressions
				identityNormalizedExpression = ExpressionTreeRewriter
						.rewriteWith(new ExpressionNodeInliner(expressionCache),
								expression);
				expressionCache.put(identityNormalizedExpression,
						identityNormalizedExpression);
			}
			return identityNormalizedExpression;
		}
	}
}
