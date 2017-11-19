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
package com.facebook.presto.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.facebook.presto.spi.pushdown.PushDown;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.sql.pair.AggOutputSignature;
import com.facebook.presto.sql.pair.AggregationPair;
import com.facebook.presto.sql.pair.Distinct2GroupbyPair;
import com.facebook.presto.sql.pair.DistinctPair;
import com.facebook.presto.sql.pair.GroupByPair;
import com.facebook.presto.sql.pair.HashOutputSignature;
import com.facebook.presto.sql.pair.LimitPair;
import com.facebook.presto.sql.pair.OrderByPair;
import com.facebook.presto.sql.pair.OutputPair;
import com.facebook.presto.sql.pair.OutputSignature;
import com.facebook.presto.sql.pair.OutputSignature.OutputType;
import com.facebook.presto.sql.pair.SimpleFilterPair;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.optimizations.OrderBy;
import com.facebook.presto.sql.planner.plan.WeiwoPushDownNode;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.airlift.log.Logger;

/**
 * push down class extractAggregationFunction: extract aggregation which
 * supports to be push down to data source
 * 
 * parseFunction
 * 
 * @author peter.wei
 */
public class PushDownUtilities {

	private static final Logger log = Logger.get(PushDownUtilities.class);

	public static boolean isPushDownFunction(Collection<Symbol> outputSymbol) {
		for (Symbol agg : outputSymbol) {
			if (agg.getName().contains("sum") || agg.getName().contains("max")
					|| agg.getName().contains("min")
					|| agg.getName().contains("avg")
					|| agg.getName().contains("count")) {
				continue;
			} else {
				return false;
			}
		}
		return true;
	}

	public static boolean isPushDownFilter(Expression node) {
		return CheckPushDownVisiterHolder.getVisitor().process(node, null);
	}

	public static String[] extractPartition(Expression node) {

		return PartitionVisiterHolder.getVisitor().process(node, null);
	}

	public static List<Symbol> extractHashFields(Expression node) {
		return FieldExtractVisiterHolder.getVisitor().process(node, null);
	}

	public static PushDown createPushDown(WeiwoPushDownNode node,
			List<Symbol> symbols) {

		Map<Symbol, List<Symbol>> hashDependency = node.getHashdependency();

		List<OutputSignature> signatures = toSignature(symbols, hashDependency);

		List<OutputSignature> aggrateSignatures = getAggregateSignature(
				signatures);

		List<OutputSignature> groupby = getGroupbySignature(signatures,
				node.getGroupbykeys());

		OutputSignature distinct2groupby = getDistinct2GroupbySignature(
				signatures, node.getDistinct2groupby());

		Distinct2GroupbyPair d2gPair = distinct2groupby == null
				? null
				: new Distinct2GroupbyPair(distinct2groupby);

		Expression filter = Optional.ofNullable(node.getOriginalConstraint())
				.orElse(BooleanLiteral.TRUE_LITERAL);

		SimpleFilterPair filterPair = new SimpleFilterPair(filter);

		AggregationPair aggregationPair = aggrateSignatures.size() == 0
				? null
				: new AggregationPair(aggrateSignatures);

		groupby = Optional.ofNullable(groupby).orElse(Collections.emptyList());

		GroupByPair groupByPair = groupby.isEmpty()
				? null
				: new GroupByPair(groupby);

		long limit = node.getLimit();

		LimitPair limitPair = limit != -1 ? new LimitPair(limit) : null;

		List<OrderBy> orderbys = node.getOrderby();

		OrderByPair orderbyPair = (orderbys == null || orderbys.size() == 0)
				? null
				: new OrderByPair(orderbys);

		List<OutputSignature> distincts = getDistinctSignature(signatures,
				node.getDistinct());

		DistinctPair distinctPair = distincts.size() == 0
				? null
				: new DistinctPair(distincts);

		OutputPair outputPair = new OutputPair(signatures);

		return new PushDown(filterPair, aggregationPair, groupByPair, limitPair,
				orderbyPair, distinctPair, outputPair, d2gPair);
	}

	// private static void toHashFieldsSignature()

	private static List<OutputSignature> toSignature(List<Symbol> symbols,
			Map<Symbol, List<Symbol>> hashDependency) {
		List<OutputSignature> output = new ArrayList<OutputSignature>();
		for (int i = 0; i < symbols.size(); i++) {
			Symbol symbol = symbols.get(i);
			if (symbol.getName().contains("count")
					|| symbol.getName().contains("sum")
					|| symbol.getName().contains("avg")
					|| symbol.getName().contains("max")
					|| symbol.getName().contains("min")) {
				output.add(new AggOutputSignature(symbol, i, BigintType.BIGINT,
						null));
			} else if (symbol.getName().contains("$hashvalue")) {
				List<Integer> dependences = new ArrayList<>();
				for (Symbol dependency : hashDependency.get(symbol)) {
					int index = symbols.indexOf(dependency);
					if (index != -1) {
						dependences.add(index);
					}
				}
				output.add(new HashOutputSignature(symbol, i, dependences));
			} else {
				output.add(new OutputSignature(symbol, i));
			}
		}
		return output;
	}

	private static List<OutputSignature> getAggregateSignature(
			List<OutputSignature> signatures) {
		List<OutputSignature> output = new ArrayList<OutputSignature>();
		for (OutputSignature signature : signatures) {
			if (signature.getKind() == OutputType.AGGREAT) {
				output.add(signature);
			}
		}
		return output;
	}

	private static List<OutputSignature> getGroupbySignature(
			List<OutputSignature> signatures, List<Symbol> groupbys) {
		List<OutputSignature> output = new ArrayList<OutputSignature>();
		for (Symbol groupby : groupbys) {
			for (OutputSignature signature : signatures) {
				if (Objects.equals(signature.getSymbol(), groupby)) {
					output.add(signature);
				}
			}
		}
		return output;
	}

	private static List<OutputSignature> getDistinctSignature(
			List<OutputSignature> signatures, Map<Symbol, Symbol> distincts) {
		List<OutputSignature> output = new ArrayList<OutputSignature>();
		for (OutputSignature signature : signatures) {
			for (Symbol symbol : distincts.values()) {
				if (symbol.getName().contains(signature.getName())) {
					output.add(signature);
				}
			}
		}
		return output;
	}

	private static OutputSignature getDistinct2GroupbySignature(
			List<OutputSignature> signatures, Symbol distinct2groupby) {
		if (distinct2groupby == null) {
			return null;
		}
		for (OutputSignature signature : signatures) {
			if (distinct2groupby.getName().contains(signature.getName())) {
				return signature;
			}
		}
		return null;
	}

	private static class PartitionVisiterHolder {

		private static PartitionVisiter visiter = new PartitionVisiter();

		static PartitionVisiter getVisitor() {
			return visiter;
		}
	}

	private static class FieldExtractVisiterHolder {

		private static FieldExtractVisiter visiter = new FieldExtractVisiter();

		static FieldExtractVisiter getVisitor() {
			return visiter;
		}
	}

	private static class CheckPushDownVisiterHolder {

		private static CheckPushDownVisiter visiter = new CheckPushDownVisiter();

		static CheckPushDownVisiter getVisitor() {
			return visiter;
		}
	}

	private static class FieldExtractVisiter
			extends
				AstVisitor<List<Symbol>, Void> {

		@Override
		protected List<Symbol> visitCoalesceExpression(CoalesceExpression node,
				Void context) {
			List<Symbol> symbols = new ArrayList<Symbol>();

			for (Expression express : node.getOperands()) {
				List<Symbol> value = process(express, context);
				if (value != null) {
					symbols.addAll(value);
				}
			}

			return symbols.stream().filter(symbol -> symbol != null)
					.collect(Collectors.toList());
		}

		@Override
		protected List<Symbol> visitFunctionCall(FunctionCall node,
				Void context) {

			List<Symbol> symbols = new ArrayList<Symbol>();

			for (Expression express : node.getArguments()) {
				List<Symbol> value = process(express, context);
				if (value != null) {
					symbols.addAll(value);
				}
			}

			return symbols.stream().filter(symbol -> symbol != null)
					.collect(Collectors.toList());
		}

		@Override
		protected List<Symbol> visitSymbolReference(SymbolReference node,
				Void context) {
			return Collections.singletonList(Symbol.from(node));
		}
	}

	private static class PartitionVisiter extends AstVisitor<String[], Void> {

		private String[] toFullorEmpty(String[] orignial) {
			return orignial == null ? new String[0] : orignial;
		}

		private String[] mergeParition(String[] left, String[] right) {
			Iterable<String> iteral = Iterables.concat(Arrays.asList(left),
					Arrays.asList(right));
			ArrayList<String> list = Lists.newArrayList(iteral.iterator());
			return list.toArray(new String[0]);
		}

		@Override
		protected String[] visitComparisonExpression(ComparisonExpression node,
				Void context) {
			String[] keys = process(node.getLeft(), null);
			if (keys != null && keys.length > 0) {
				if (node.getType() == ComparisonExpression.Type.EQUAL) {
					Expression value = node.getRight();
					String[] partitions = process(value, context);
					return toFullorEmpty(partitions);
				} else {
					return toFullorEmpty(null);
				}
			}
			return toFullorEmpty(null);
		}

		@Override
		protected String[] visitDereferenceExpression(
				DereferenceExpression node, Void context) {
			String fieldName = node.getFieldName();
			if ("vpartition".equalsIgnoreCase(fieldName)) {
				return new String[]{fieldName};
			}
			return toFullorEmpty(null);
		}

		@Override
		protected String[] visitBooleanLiteral(BooleanLiteral node,
				Void context) {
			return toFullorEmpty(null);
		}

		@Override
		protected String[] visitLogicalBinaryExpression(
				LogicalBinaryExpression node, Void context) {
			String[] leftPartitions = this.process(node.getLeft(), context);
			String[] rightPartitions = this.process(node.getRight(), context);
			return mergeParition(toFullorEmpty(leftPartitions),
					toFullorEmpty(rightPartitions));
		}

		@Override
		protected String[] visitStringLiteral(StringLiteral node,
				Void context) {
			return new String[]{node.getValue()};
		}

		@Override
		protected String[] visitInPredicate(InPredicate node, Void context) {

			String[] keys = process(node.getValue(), context);
			if (keys != null && keys.length > 0) {
				String[] partitions = process(node.getValueList(), context);
				return toFullorEmpty(partitions);
			} else {
				return toFullorEmpty(keys);
			}
		}

		@Override
		protected String[] visitQualifiedNameReference(
				QualifiedNameReference node, Void context) {
			// not totally true need to be refactor
			String name = node.getName().getSuffix();
			if (name.toLowerCase().startsWith("vpartition")) {
				return new String[]{name};
			}
			return toFullorEmpty(null);
		}

		@Override
		protected String[] visitCast(Cast node, Void context) {
			return process(node.getExpression(), context);
		}

		@Override
		protected String[] visitSymbolReference(SymbolReference node,
				Void context) {
			String name = node.getName();
			if (name.toLowerCase().startsWith("vpartition")) {
				return new String[]{name};
			}
			return toFullorEmpty(null);
		}

		@Override
		protected String[] visitInListExpression(InListExpression node,
				Void context) {
			List<String> partitions = new LinkedList<String>();
			for (Expression item : node.getValues()) {
				partitions.addAll(Arrays.asList(process(item, context)));
			}
			return partitions.toArray(new String[0]);
		}
	}

	public static class CheckPushDownVisiter extends AstVisitor<Boolean, Void> {

		@Override
		protected Boolean visitNode(Node node, Void context) {
			return false;
		}

		@Override
		protected Boolean visitLikePredicate(LikePredicate node, Void context) {
			return true;
		}

		@Override
		protected Boolean visitComparisonExpression(ComparisonExpression node,
				Void context) {
			boolean left = process(node.getLeft(), context);
			boolean right = process(node.getRight(), context);
			return left && right;
		}

		@Override
		protected Boolean visitFunctionCall(FunctionCall node, Void context) {

			return true;
		}

		@Override
		protected Boolean visitLogicalBinaryExpression(
				LogicalBinaryExpression node, Void context) {
			boolean left = this.process(node.getLeft(), context);
			boolean right = this.process(node.getRight(), context);
			return left && right;
		}

		@Override
		protected Boolean visitNotExpression(NotExpression node, Void context) {
			return this.process(node.getValue(), context);
		}

		@Override
		protected Boolean visitBetweenPredicate(BetweenPredicate node,
				Void context) {
			boolean left = process(node.getMin(), context);
			boolean right = process(node.getMax(), context);
			return left && right;
		}

		@Override
		protected Boolean visitNullLiteral(NullLiteral node, Void context) {
			return true;
		}

		@Override
		protected Boolean visitIsNotNullPredicate(IsNotNullPredicate node,
				Void context) {
			return process(node.getValue(), context);
		}

		@Override
		protected Boolean visitIsNullPredicate(IsNullPredicate node,
				Void context) {
			return process(node.getValue(), context);
		}

		@Override
		protected Boolean visitStringLiteral(StringLiteral node, Void context) {
			log.debug("I am visitStringLiteral " + node);
			return true;
		}

		@Override
		protected Boolean visitLongLiteral(LongLiteral node, Void context) {
			log.debug("I am visitLongLiteral " + node);
			return true;
		}

		@Override
		protected Boolean visitDoubleLiteral(DoubleLiteral node, Void context) {
			log.debug("I am visitDoubleLiteral " + node);
			return true;
		}

		@Override
		protected Boolean visitCast(Cast node, Void context) {
			return true;
		}

		@Override
		protected Boolean visitInPredicate(InPredicate node, Void context) {
			return true;
		}

		@Override
		protected Boolean visitLiteral(Literal node, Void context) {
			log.debug("I am visitLiteral " + node);
			return true;
		}

		@Override
		protected Boolean visitGenericLiteral(GenericLiteral node,
				Void context) {
			return true;
		}

		@Override
		protected Boolean visitIntervalLiteral(IntervalLiteral node,
				Void context) {
			log.debug("I am visitIntervalLiteral " + node);
			return true;
		}

		@Override
		protected Boolean visitBinaryLiteral(BinaryLiteral node, Void context) {
			log.debug("I am visitBinaryLiteral " + node);
			return true;
		}

		@Override
		protected Boolean visitQualifiedNameReference(
				QualifiedNameReference node, Void context) {
			log.debug("I am visitQualifiedNameReference " + node);
			return true;
		}

		@Override
		protected Boolean visitInListExpression(InListExpression node,
				Void context) {
			log.debug("I am visitInListExpression " + node);
			return true;
		}

		@Override
		protected Boolean visitFieldReference(FieldReference node,
				Void context) {
			log.debug("I am fieldReference " + node);
			return true;
		}

		@Override
		protected Boolean visitSymbolReference(SymbolReference node,
				Void context) {
			log.debug("I am symbolReference " + node);
			return true;
		}
	}
}