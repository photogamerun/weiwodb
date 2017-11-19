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
package com.facebook.presto.weiwo.manager;

import java.util.List;

import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.WeiwoPushDownNode;
/**
 * 
 * simple implement to find weiwoNode, if exit, push down logic will be
 * triggered.
 * 
 * @author peter.wei
 *
 */
public class WeiwoNodeFinder extends PlanVisitor<Class<?>, Boolean> {

	@Override
	public Boolean visitAggregation(AggregationNode node, Class<?> context) {
		return node.getSource().accept(this, context);
	}

	@Override
	public Boolean visitFilter(FilterNode node, Class<?> context) {
		return node.getSource().accept(this, context);
	}

	@Override
	public Boolean visitProject(ProjectNode node, Class<?> context) {
		return node.getSource().accept(this, context);
	}

	@Override
	public Boolean visitLimit(LimitNode node, Class<?> context) {
		return node.getSource().accept(this, context);
	}

	@Override
	public Boolean visitSort(SortNode node, Class<?> context) {
		return node.getSource().accept(this, context);
	}

	@Override
	protected Boolean visitPlan(PlanNode node, Class<?> context) {
		List<PlanNode> planNodes = node.getSources();
		boolean isExistd = true;

		for (PlanNode planNode : planNodes) {
			isExistd &= planNode.accept(this, context);
		}

		return isExistd;
	}

	@Override
	public Boolean visitJoin(JoinNode node, Class<?> context) {
		return false;
	}

	@Override
	public Boolean visitRemoteSource(RemoteSourceNode node, Class<?> context) {
		return false;
	}

	@Override
	public Boolean visitExchange(ExchangeNode node, Class<?> context) {
		return false;
	}

	@Override
	public Boolean visitWeiwoPushDown(WeiwoPushDownNode node,
			Class<?> context) {
		return node.getClass().isAssignableFrom(context);
	}

	@Override
	public Boolean visitTableScan(TableScanNode node, Class<?> context) {
		return node.getClass().isAssignableFrom(context);
	}
}
