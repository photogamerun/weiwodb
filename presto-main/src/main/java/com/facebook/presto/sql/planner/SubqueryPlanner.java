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

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN;
import static com.facebook.presto.sql.util.AstUtils.nodeContains;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

class SubqueryPlanner {
	private final Analysis analysis;
	private final SymbolAllocator symbolAllocator;
	private final PlanNodeIdAllocator idAllocator;
	private final Metadata metadata;
	private final Session session;

	SubqueryPlanner(Analysis analysis, SymbolAllocator symbolAllocator,
			PlanNodeIdAllocator idAllocator, Metadata metadata,
			Session session) {
		requireNonNull(analysis, "analysis is null");
		requireNonNull(symbolAllocator, "symbolAllocator is null");
		requireNonNull(idAllocator, "idAllocator is null");
		requireNonNull(metadata, "metadata is null");
		requireNonNull(session, "session is null");

		this.analysis = analysis;
		this.symbolAllocator = symbolAllocator;
		this.idAllocator = idAllocator;
		this.metadata = metadata;
		this.session = session;
	}

	public PlanBuilder handleSubqueries(PlanBuilder builder,
			Collection<Expression> expressions, Node node) {
		for (Expression expression : expressions) {
			builder = handleSubqueries(builder, expression, node);
		}
		return builder;
	}

	public PlanBuilder handleSubqueries(PlanBuilder builder,
			Expression expression, Node node) {
		builder = appendInPredicateApplyNodes(builder,
				analysis.getInPredicateSubqueries(node).stream()
						.filter(inPredicate -> nodeContains(expression,
								inPredicate.getValueList()))
						.collect(toImmutableSet()));
		builder = appendScalarSubqueryApplyNodes(builder,
				analysis.getScalarSubqueries(node).stream()
						.filter(subquery -> nodeContains(expression, subquery))
						.collect(toImmutableSet()));
		builder = appendExistsSubqueryApplyNodes(builder,
				analysis.getExistsSubqueries(node).stream()
						.filter(subquery -> nodeContains(expression, subquery))
						.collect(toImmutableSet()));
		return builder;
	}

	private PlanBuilder appendInPredicateApplyNodes(PlanBuilder subPlan,
			Set<InPredicate> inPredicates) {
		for (InPredicate inPredicate : inPredicates) {
			subPlan = appendInPredicateApplyNode(subPlan, inPredicate);
		}
		return subPlan;
	}

	private PlanBuilder appendInPredicateApplyNode(PlanBuilder subPlan,
			InPredicate inPredicate) {
		subPlan = subPlan.appendProjections(
				ImmutableList.of(inPredicate.getValue()), symbolAllocator,
				idAllocator);

		checkState(inPredicate.getValueList() instanceof SubqueryExpression);
		SubqueryExpression subqueryExpression = (SubqueryExpression) inPredicate
				.getValueList();
		RelationPlan valueListRelation = createRelationPlan(
				subqueryExpression.getQuery());

		TranslationMap translationMap = subPlan.copyTranslations();
		SymbolReference valueList = getOnlyElement(
				valueListRelation.getOutputSymbols()).toSymbolReference();
		translationMap.put(inPredicate,
				new InPredicate(inPredicate.getValue(), valueList));

		return new PlanBuilder(translationMap,
				// TODO handle correlation
				new ApplyNode(idAllocator.getNextId(), subPlan.getRoot(),
						valueListRelation.getRoot(), ImmutableList.of()),
				subPlan.getSampleWeight());
	}

	private PlanBuilder appendScalarSubqueryApplyNodes(PlanBuilder builder,
			Set<SubqueryExpression> scalarSubqueries) {
		for (SubqueryExpression scalarSubquery : scalarSubqueries) {
			builder = appendScalarSubqueryApplyNode(builder, scalarSubquery);
		}
		return builder;
	}

	private PlanBuilder appendScalarSubqueryApplyNode(PlanBuilder subPlan,
			SubqueryExpression scalarSubquery) {
		return appendSubqueryApplyNode(subPlan, scalarSubquery,
				scalarSubquery.getQuery(),
				subquery -> new EnforceSingleRowNode(idAllocator.getNextId(),
						createRelationPlan(subquery).getRoot()));
	}

	private PlanBuilder appendExistsSubqueryApplyNodes(PlanBuilder builder,
			Set<ExistsPredicate> existsPredicates) {
		for (ExistsPredicate existsPredicate : existsPredicates) {
			builder = appendExistSubqueryApplyNode(builder, existsPredicate);
		}
		return builder;
	}

	/**
	 * Exists is modeled as:
	 * 
	 * <pre>
	 *     - EnforceSingleRow
	 *       - Project($0 > 0)
	 *         - Aggregation(COUNT(*))
	 *           - Limit(1)
	 *             -- subquery
	 * </pre>
	 */
	private PlanBuilder appendExistSubqueryApplyNode(PlanBuilder subPlan,
			ExistsPredicate existsPredicate) {
		return appendSubqueryApplyNode(subPlan, existsPredicate,
				existsPredicate.getSubquery(), subquery -> {
					PlanNode subqueryPlan = createRelationPlan(subquery)
							.getRoot();

					subqueryPlan = new LimitNode(idAllocator.getNextId(),
							subqueryPlan, 1, false);

					FunctionRegistry functionRegistry = metadata
							.getFunctionRegistry();
					QualifiedName countFunction = QualifiedName.of("count");
					Symbol count = symbolAllocator
							.newSymbol(countFunction.toString(), BIGINT);
					subqueryPlan = new AggregationNode(idAllocator.getNextId(),
							subqueryPlan, ImmutableList.of(),
							ImmutableMap.of(count,
									new FunctionCall(countFunction,
											ImmutableList.of())),
							ImmutableMap.of(count,
									functionRegistry.resolveFunction(
											countFunction, ImmutableList.of(),
											false)),
							ImmutableMap.of(),
							ImmutableList.of(ImmutableList.of()),
							AggregationNode.Step.SINGLE, Optional.empty(), 1.0,
							Optional.empty(), null);

					Symbol exists = symbolAllocator.newSymbol("exists",
							BOOLEAN);
					ComparisonExpression countGreaterThanZero = new ComparisonExpression(
							GREATER_THAN, count.toSymbolReference(),
							new Cast(new LongLiteral("0"), BIGINT.toString()));
					return new EnforceSingleRowNode(idAllocator.getNextId(),
							new ProjectNode(idAllocator.getNextId(),
									subqueryPlan, ImmutableMap.of(exists,
											countGreaterThanZero)));
				});
	}

	private PlanBuilder appendSubqueryApplyNode(PlanBuilder subPlan,
			Expression subqueryExpression, Query subquery,
			Function<Query, PlanNode> subqueryPlanner) {
		if (subPlan.canTranslate(subqueryExpression)) {
			// given subquery is already appended
			return subPlan;
		}

		PlanNode subqueryNode = subqueryPlanner.apply(subquery);

		TranslationMap translations = subPlan.copyTranslations();
		translations.put(subqueryExpression,
				getOnlyElement(subqueryNode.getOutputSymbols()));

		PlanNode root = subPlan.getRoot();
		if (root.getOutputSymbols().isEmpty()) {
			// there is nothing to join with - e.g. SELECT (SELECT 1)
			return new PlanBuilder(translations, subqueryNode,
					subPlan.getSampleWeight());
		} else {
			return new PlanBuilder(translations,
					// TODO handle parameter list
					new ApplyNode(idAllocator.getNextId(), root, subqueryNode,
							ImmutableList.of()),
					subPlan.getSampleWeight());
		}
	}

	private RelationPlan createRelationPlan(Query subquery) {
		return new RelationPlanner(analysis, symbolAllocator, idAllocator,
				metadata, session).process(subquery, null);
	}
}
