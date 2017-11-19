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
package com.facebook.presto.sql.planner.optimizations;

import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IntersectNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;

/**
 * Converts INTERSECT query into UNION ALL..GROUP BY...WHERE Eg: SELECT a FROM
 * foo INTERSECT SELECT x FROM bar
 * <p/>
 * =>
 * <p/>
 * SELECT a FROM (SELECT a, COUNT(foo_marker) AS foo_cnt, COUNT(bar_marker) AS
 * bar_cnt FROM ( SELECT a, true as foo_marker, null as bar_marker FROM foo
 * UNION ALL SELECT x, null as foo_marker, true as bar_marker FROM bar ) T1
 * GROUP BY a) T2 WHERE foo_cnt >= 1 AND bar_cnt >= 1;
 */
public class ImplementIntersectAsUnion implements PlanOptimizer {
	@Override
	public PlanNode optimize(PlanNode plan, Session session,
			Map<Symbol, Type> types, SymbolAllocator symbolAllocator,
			PlanNodeIdAllocator idAllocator) {
		requireNonNull(plan, "plan is null");
		requireNonNull(session, "session is null");
		requireNonNull(types, "types is null");
		requireNonNull(symbolAllocator, "symbolAllocator is null");
		requireNonNull(idAllocator, "idAllocator is null");

		return SimplePlanRewriter
				.rewriteWith(new Rewriter(idAllocator, symbolAllocator), plan);
	}

	private static class Rewriter extends SimplePlanRewriter<Void> {
		private static final String INTERSECT_MARKER = "intersect_marker";
		private static final Signature COUNT_AGGREGATION = new Signature(
				"count", AGGREGATE, parseTypeSignature(StandardTypes.BIGINT),
				parseTypeSignature(StandardTypes.BOOLEAN));
		private final PlanNodeIdAllocator idAllocator;
		private final SymbolAllocator symbolAllocator;

		private Rewriter(PlanNodeIdAllocator idAllocator,
				SymbolAllocator symbolAllocator) {
			this.idAllocator = requireNonNull(idAllocator,
					"idAllocator is null");
			this.symbolAllocator = requireNonNull(symbolAllocator,
					"symbolAllocator is null");
		}

		@Override
		public PlanNode visitIntersect(IntersectNode node,
				RewriteContext<Void> rewriteContext) {
			List<PlanNode> sources = node.getSources().stream()
					.map(rewriteContext::rewrite).collect(toList());

			List<Symbol> markers = allocateMarkers(sources.size());

			// identity projection for all the fields in each of the sources
			// plus marker columns
			List<PlanNode> withMarkers = appendMarkers(markers, sources, node);

			// add a union over all the rewritten sources. The outputs of the
			// union have the same name as the
			// original intersect node
			List<Symbol> outputs = node.getOutputSymbols();
			UnionNode union = union(withMarkers,
					ImmutableList.copyOf(concat(outputs, markers)));

			// add count aggregations and filter rows where any of the counts is
			// >= 1
			AggregationNode aggregation = computeCounts(union, outputs,
					markers);
			FilterNode filterNode = addFilter(aggregation);

			return project(filterNode, outputs);
		}

		private List<Symbol> allocateMarkers(int count) {
			ImmutableList.Builder<Symbol> markers = ImmutableList.builder();
			for (int i = 0; i < count; i++) {
				markers.add(
						symbolAllocator.newSymbol(INTERSECT_MARKER, BOOLEAN));
			}
			return markers.build();
		}

		private List<PlanNode> appendMarkers(List<Symbol> markers,
				List<PlanNode> nodes, IntersectNode intersect) {
			ImmutableList.Builder<PlanNode> result = ImmutableList.builder();
			for (int i = 0; i < nodes.size(); i++) {
				result.add(appendMarkers(nodes.get(i), i, markers,
						intersect.sourceSymbolMap(i)));
			}
			return result.build();
		}

		private PlanNode appendMarkers(PlanNode source, int markerIndex,
				List<Symbol> markers,
				Map<Symbol, SymbolReference> projections) {
			ImmutableMap.Builder<Symbol, Expression> assignments = ImmutableMap
					.builder();
			// add existing intersect symbols to projection
			for (Map.Entry<Symbol, SymbolReference> entry : projections
					.entrySet()) {
				Symbol symbol = symbolAllocator.newSymbol(
						entry.getKey().getName(),
						symbolAllocator.getTypes().get(entry.getKey()));
				assignments.put(symbol, entry.getValue());
			}

			// add extra marker fields to the projection
			for (int i = 0; i < markers.size(); ++i) {
				Expression expression = (i == markerIndex)
						? TRUE_LITERAL
						: new Cast(new NullLiteral(), StandardTypes.BOOLEAN);
				assignments.put(symbolAllocator.newSymbol(
						markers.get(i).getName(), BOOLEAN), expression);
			}

			return new ProjectNode(idAllocator.getNextId(), source,
					assignments.build());
		}

		private UnionNode union(List<PlanNode> nodes, List<Symbol> outputs) {
			ImmutableListMultimap.Builder<Symbol, Symbol> outputsToInputs = ImmutableListMultimap
					.builder();
			for (PlanNode source : nodes) {
				for (int i = 0; i < source.getOutputSymbols().size(); i++) {
					outputsToInputs.put(outputs.get(i),
							source.getOutputSymbols().get(i));
				}
			}

			return new UnionNode(idAllocator.getNextId(), nodes,
					outputsToInputs.build(), outputs);
		}

		private AggregationNode computeCounts(UnionNode sourceNode,
				List<Symbol> originalColumns, List<Symbol> markers) {
			ImmutableMap.Builder<Symbol, Signature> signatures = ImmutableMap
					.builder();
			ImmutableMap.Builder<Symbol, FunctionCall> aggregations = ImmutableMap
					.builder();

			for (Symbol marker : markers) {
				Symbol output = symbolAllocator.newSymbol("count", BIGINT);
				aggregations.put(output,
						new FunctionCall(QualifiedName.of("count"),
								ImmutableList.of(marker.toSymbolReference())));
				signatures.put(output, COUNT_AGGREGATION);
			}

			return new AggregationNode(idAllocator.getNextId(), sourceNode,
					originalColumns, aggregations.build(), signatures.build(),
					ImmutableMap.<Symbol, Symbol> of(),
					ImmutableList.of(originalColumns), Step.SINGLE,
					Optional.empty(), 1.0, Optional.empty(), null);
		}

		private FilterNode addFilter(AggregationNode aggregation) {
			ImmutableList<Expression> predicates = aggregation.getAggregations()
					.keySet().stream()
					.map(column -> new ComparisonExpression(
							GREATER_THAN_OR_EQUAL, column.toSymbolReference(),
							new GenericLiteral("BIGINT", "1")))
					.collect(toImmutableList());
			return new FilterNode(idAllocator.getNextId(), aggregation,
					ExpressionUtils.and(predicates));
		}

		private ProjectNode project(PlanNode node, List<Symbol> columns) {
			return new ProjectNode(idAllocator.getNextId(), node,
					columns.stream().collect(toMap(Function.identity(),
							Symbol::toSymbolReference)));
		}
	}
}
