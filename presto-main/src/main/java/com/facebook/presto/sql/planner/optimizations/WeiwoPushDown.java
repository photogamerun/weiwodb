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

import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.facebook.presto.Session;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.WeiwoPushDownNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.util.PushDownUtilities;
import com.facebook.presto.weiwo.manager.WeiwoNodeFinder;
import com.google.common.base.MoreObjects;
/**
 * 
 * @author peter.wei
 *
 */
public class WeiwoPushDown implements PlanOptimizer {

	@Override
	public PlanNode optimize(PlanNode plan, Session session,
			Map<Symbol, Type> types, SymbolAllocator symbolAllocator,
			PlanNodeIdAllocator idAllocator) {
		requireNonNull(plan, "plan is null");
		requireNonNull(session, "session is null");
		requireNonNull(types, "types is null");
		requireNonNull(symbolAllocator, "symbolAllocator is null");
		requireNonNull(idAllocator, "idAllocator is null");

		return SimplePlanRewriter.rewriteWith(new Rewriter(), plan, null);
	}

	private static class WeiwoContext {

		private long limit = -1;
		private List<OrderBy> orderbys = new LinkedList<OrderBy>();
		private List<Symbol> groupbys = new LinkedList<Symbol>();
		private Expression express;
		private Map<Symbol, Symbol> distincts = new HashMap<Symbol, Symbol>();
		private boolean pushdown = true;
		private List<Symbol> outputs = new ArrayList<Symbol>();
		private Symbol distinct2groupby;
		private Map<Symbol, List<Symbol>> hashDependences = new HashMap<Symbol, List<Symbol>>();

		public void putHashDependences(Map<Symbol, List<Symbol>> dependence) {
			hashDependences.putAll(dependence);
		}

		public Map<Symbol, List<Symbol>> getHashDependences() {
			return hashDependences;
		}

		@Override
		public String toString() {
			return MoreObjects.toStringHelper(this).add("limit", limit)
					.add("orderbys", orderbys).add("groupbys", groupbys)
					.toString();
		}

		public void setOutputSymbol(List<Symbol> outputs) {
			this.outputs = outputs;
		}

		public List<Symbol> getOutputs() {
			return outputs;
		}

		public void setPushdown(boolean isPushdown) {
			this.pushdown &= isPushdown;
		}

		public boolean isPushdown() {
			return pushdown;
		}

		public Expression getExpress() {
			return express;
		}

		public void setExpress(Expression express) {
			this.express = express;
		}

		public List<Symbol> getGroupbys() {
			return groupbys;
		}

		public void setGroupbys(List<Symbol> groupbys) {
			this.groupbys = groupbys;
		}

		public long getLimit() {
			return limit;
		}

		public void setLimit(long limit) {
			this.limit = limit;
		}

		public List<OrderBy> getOrderbys() {
			return orderbys;
		}

		public void addOrderby(OrderBy orderby) {
			this.orderbys.add(orderby);
		}

		public Map<Symbol, Symbol> getDistincts() {
			return distincts;
		}

		public void addDistincts(Map<Symbol, Symbol> distincts) {
			this.distincts.putAll(distincts);
		}

		public Symbol getDistinct2groupby() {
			return distinct2groupby;
		}

		public void setDistinct2groupby(Symbol distinct2groupby) {
			this.distinct2groupby = distinct2groupby;
		}
	}

	private static class Rewriter extends SimplePlanRewriter<WeiwoContext> {

		@Override
		public PlanNode visitLimit(LimitNode node,
				RewriteContext<WeiwoContext> context) {
			long count = node.getCount();
			WeiwoContext weiwoContext = null;
			if (count > 0) {
				weiwoContext = context.get();
				if (weiwoContext == null) {
					weiwoContext = new WeiwoContext();
				}
				weiwoContext.setLimit(count);
			}
			return context.defaultRewrite(node, weiwoContext);
		}

		@Override
		public PlanNode visitSort(SortNode node,
				RewriteContext<WeiwoContext> context) {
			Map<Symbol, SortOrder> ordering = node.getOrderings();
			WeiwoContext weiwoContext = null;
			if (ordering.size() > 0) {
				weiwoContext = context.get();
				if (weiwoContext == null) {
					weiwoContext = new WeiwoContext();
				}
			}

			for (Symbol symbol : node.getOrderBy()) {
				weiwoContext.addOrderby(
						new OrderBy(symbol, ordering.get(symbol).name()));
			}

			return context.defaultRewrite(node, weiwoContext);
		}

		@Override
		public PlanNode visitTopN(TopNNode node,
				RewriteContext<WeiwoContext> context) {
			WeiwoContext weiwoContext = context.get();
			if (weiwoContext == null) {
				weiwoContext = new WeiwoContext();
			}
			weiwoContext.setLimit(node.getCount());
			Map<Symbol, SortOrder> ordering = node.getOrderings();
			for (Symbol symbol : node.getOrderBy()) {
				weiwoContext.addOrderby(
						new OrderBy(symbol, ordering.get(symbol).name()));
			}
			return context.defaultRewrite(node, weiwoContext);
		}

		@Override
		public PlanNode visitFilter(FilterNode node,
				RewriteContext<WeiwoContext> context) {
			WeiwoContext weiwoContext = context.get();
			if (weiwoContext == null) {
				weiwoContext = new WeiwoContext();
			}
			weiwoContext.setPushdown(
					PushDownUtilities.isPushDownFilter(node.getPredicate()));

			weiwoContext.setExpress(node.getPredicate());
			return context.defaultRewrite(node, weiwoContext);
		}

		@Override
		public PlanNode visitTableScan(TableScanNode node,
				RewriteContext<WeiwoContext> context) {

			if (Objects.equals("weiwodb", node.getTable().getConnectorId())) {
				WeiwoContext weiwContext = context.get();
				if (weiwContext == null ? false : weiwContext.isPushdown()) {
					long limit = weiwContext.getLimit();
					List<OrderBy> orderby = weiwContext.getOrderbys();
					List<Symbol> groupbykeys = weiwContext.getGroupbys();
					Expression originalConstraint = weiwContext.getExpress();

					originalConstraint = originalConstraint == null
							? node.getOriginalConstraint()
							: originalConstraint;

					return new WeiwoPushDownNode(node.getId(), limit, orderby,
							node.getTable(), node.getOutputSymbols(),
							node.getAssignments(), node.getLayout(),
							node.getCurrentConstraint(), originalConstraint,
							groupbykeys, weiwContext.getDistincts(),
							weiwContext.getOutputs(),
							weiwContext.getDistinct2groupby(),
							weiwContext.getHashDependences());
				}
			}
			return super.visitTableScan(node, context);
		}

		private WeiwoNodeFinder nodeFinder = new WeiwoNodeFinder();

		@Override
		public PlanNode visitExchange(ExchangeNode node,
				RewriteContext<WeiwoContext> context) {
			List<PlanNode> childrens = new ArrayList<PlanNode>();

			for (PlanNode remoteNode : node.getSources()) {
				if (remoteNode.accept(nodeFinder, TableScanNode.class)) {
					WeiwoContext remoteContext = context.get();
					if (remoteContext == null) {
						remoteContext = new WeiwoContext();
					}
					remoteContext
							.setOutputSymbol(remoteNode.getOutputSymbols());
					childrens.add(context.rewrite(remoteNode, remoteContext));
				} else {
					childrens.add(context.rewrite(remoteNode, context.get()));
				}
			}
			return replaceChildren(node, childrens);
		}

		@Override
		public PlanNode visitAggregation(AggregationNode node,
				RewriteContext<WeiwoContext> context) {
			WeiwoContext weiwoContext = context.get();
			if (weiwoContext == null) {
				weiwoContext = new WeiwoContext();
			}

			if (!node.getFunctions().isEmpty()) {
				weiwoContext.setPushdown(PushDownUtilities
						.isPushDownFunction(node.getFunctions().keySet()));
			}

			Map<Symbol, Symbol> masks = node.getMasks();
			List<Symbol> groups = node.getGroupBy();

			if (groups == null || groups.isEmpty()) {
			} else {
				weiwoContext.setGroupbys(node.getGroupBy());
			}

			if (!masks.isEmpty()) {
				weiwoContext.addDistincts(node.getMasks());
			}

			// 处理distinct 字段被优化器下放到group by 逻辑中的情况

			weiwoContext.setDistinct2groupby(node.getDistinct2group());

			return context.defaultRewrite(node, weiwoContext);
		}

		@Override
		public PlanNode visitProject(ProjectNode node,
				RewriteContext<WeiwoContext> context) {
			WeiwoContext weiwoContext = context.get();
			if (weiwoContext == null) {
				weiwoContext = new WeiwoContext();
			}
			weiwoContext.putHashDependences(gethashdependens(node));
			return context.defaultRewrite(node, weiwoContext);
		}

		private Map<Symbol, List<Symbol>> gethashdependens(ProjectNode node) {
			Map<Symbol, List<Symbol>> hash2Fields = new HashMap<Symbol, List<Symbol>>();
			List<Symbol> outputs = node.getOutputSymbols();
			Map<Symbol, Expression> assignments = node.getAssignments();
			for (Symbol hashSymbol : outputs) {
				if (hashSymbol.getName().contains("$hashvalue")) {
					Expression expression = assignments.get(hashSymbol);
					if (expression != null) {
						for (Symbol field : PushDownUtilities
								.extractHashFields(expression)) {
							if (field != null) {
								List<Symbol> dependences = hash2Fields
										.get(hashSymbol);
								if (dependences == null) {
									dependences = new ArrayList<>();
									dependences.add(field);
									hash2Fields.put(hashSymbol, dependences);
								} else {
									dependences.add(field);
								}
							}
						}
					}
				}
			}
			return hash2Fields;
		}
	}
}