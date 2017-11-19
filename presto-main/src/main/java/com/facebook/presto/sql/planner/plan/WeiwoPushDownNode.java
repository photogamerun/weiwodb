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
package com.facebook.presto.sql.planner.plan;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.optimizations.OrderBy;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * 
 * Weiwo push down node
 * 
 * @author peter.wei
 *
 */
@Immutable
public class WeiwoPushDownNode extends PlanNode {

	private long limit;

	private List<OrderBy> orderby;

	private final TableHandle table;
	private final Optional<TableLayoutHandle> tableLayout;
	private final List<Symbol> outputSymbols;
	private final Map<Symbol, ColumnHandle> assignments;
	private final TupleDomain<ColumnHandle> currentConstraint;
	private final Expression originalConstraint;
	private final List<Symbol> groupbykeys;
	private final Map<Symbol, Symbol> distinct;
	private final List<Symbol> weiwoOutputSymbols;
	private final Symbol distinct2groupby;
	private final Map<Symbol, List<Symbol>> hashdependency;

	@JsonCreator
	public WeiwoPushDownNode(@JsonProperty("id") PlanNodeId id,
			@JsonProperty("limit") long limit,
			@JsonProperty("orderby") List<OrderBy> orderby,
			@JsonProperty("table") TableHandle table,
			@JsonProperty("outputSymbols") List<Symbol> outputs,
			@JsonProperty("assignments") Map<Symbol, ColumnHandle> assignments,
			@JsonProperty("layout") Optional<TableLayoutHandle> tableLayout,
			@JsonProperty("currentConstraint") TupleDomain<ColumnHandle> currentConstraint,
			@JsonProperty("originalConstraint") @Nullable Expression originalConstraint,
			@JsonProperty("groupbykeys") @Nullable List<Symbol> groupbykeys,
			@JsonProperty("distinct") @Nullable Map<Symbol, Symbol> distinct,
			@JsonProperty("weiwoOutputSymbols") List<Symbol> weiwoOutputs,
			@JsonProperty("distinct2groupby") @Nullable Symbol distinct2groupby,
			@JsonProperty("hashdependency") @Nullable Map<Symbol, List<Symbol>> hashdependency) {
		super(id);
		requireNonNull(table, "table is null");
		requireNonNull(outputs, "outputs is null");
		requireNonNull(assignments, "assignments is null");
		Preconditions.checkArgument(assignments.keySet().containsAll(outputs),
				"assignments does not cover all of outputs");
		requireNonNull(tableLayout, "tableLayout is null");
		requireNonNull(currentConstraint, "currentConstraint is null");
		this.weiwoOutputSymbols = requireNonNull(weiwoOutputs,
				"weiwoOutputs is null");
		this.table = requireNonNull(table, "table is null");
		this.limit = requireNonNull(limit, "limit is null");
		this.orderby = requireNonNull(orderby, "orderby is null");
		this.outputSymbols = ImmutableList.copyOf(outputs);
		this.assignments = ImmutableMap.copyOf(assignments);
		this.originalConstraint = originalConstraint;
		this.tableLayout = tableLayout;
		this.currentConstraint = currentConstraint;
		this.groupbykeys = groupbykeys;
		this.distinct = distinct;
		this.distinct2groupby = distinct2groupby;
		this.hashdependency = hashdependency;
	}

	@JsonProperty("hashdependency")
	public Map<Symbol, List<Symbol>> getHashdependency() {
		return hashdependency;
	}

	@JsonProperty("distinct2groupby")
	public Symbol getDistinct2groupby() {
		return distinct2groupby;
	}

	@JsonProperty("weiwoOutputSymbols")
	public List<Symbol> getWeiwoOutputSymbols() {
		return weiwoOutputSymbols;
	}

	@JsonProperty("distinct")
	public Map<Symbol, Symbol> getDistinct() {
		return distinct;
	}

	@JsonProperty("groupbykeys")
	public List<Symbol> getGroupbykeys() {
		return groupbykeys;
	}

	@Override
	@JsonProperty("outputSymbols")
	public List<Symbol> getOutputSymbols() {
		return outputSymbols;
	}

	@JsonProperty("limit")
	public long getLimit() {
		return limit;
	}

	@JsonProperty("orderby")
	public List<OrderBy> getOrderby() {
		return orderby;
	}
	@Override
	public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
		return visitor.visitWeiwoPushDown(this, context);
	}

	@Override
	public List<PlanNode> getSources() {
		return ImmutableList.of();
	}

	@JsonProperty("table")
	public TableHandle getTable() {
		return table;
	}

	@JsonProperty("layout")
	public Optional<TableLayoutHandle> getLayout() {
		return tableLayout;
	}

	@JsonProperty("assignments")
	public Map<Symbol, ColumnHandle> getAssignments() {
		return assignments;
	}

	@JsonProperty("currentConstraint")
	public TupleDomain<ColumnHandle> getCurrentConstraint() {
		return currentConstraint;
	}

	@JsonProperty("originalConstraint")
	public Expression getOriginalConstraint() {
		return originalConstraint;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("table", table)
				.add("tableLayout", tableLayout)
				.add("outputSymbols", outputSymbols)
				.add("assignments", assignments)
				.add("currentConstraint", currentConstraint)
				.add("originalConstraint", originalConstraint)
				.add("limit", limit).add("orderby", orderby)
				.add("groupbykeys", groupbykeys).add("distinct", distinct)
				.toString();
	}
}