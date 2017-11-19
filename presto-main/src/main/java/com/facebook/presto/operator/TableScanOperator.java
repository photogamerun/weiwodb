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
package com.facebook.presto.operator;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.pushdown.PushDown;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.pair.SimpleFilterPair;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.util.PushDownUtilities;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import io.airlift.log.Logger;

public class TableScanOperator implements SourceOperator, Closeable {

	private static final Logger log = Logger.get(TableScanOperator.class);

	public static class TableScanOperatorFactory
			implements
				SourceOperatorFactory {
		private final int operatorId;
		private final PlanNodeId sourceId;
		private final PageSourceProvider pageSourceProvider;
		private final List<Type> types;
		private final List<ColumnHandle> columns;
		private PushDown pushDown;
		private boolean closed;

		public TableScanOperatorFactory(int operatorId, PlanNodeId sourceId,
				PageSourceProvider pageSourceProvider, List<Type> types,
				Iterable<ColumnHandle> columns) {
			this.operatorId = operatorId;
			this.sourceId = requireNonNull(sourceId, "sourceId is null");
			this.types = requireNonNull(types, "types is null");
			this.pageSourceProvider = requireNonNull(pageSourceProvider,
					"pageSourceProvider is null");
			this.columns = ImmutableList
					.copyOf(requireNonNull(columns, "columns is null"));
		}

		public TableScanOperatorFactory(int operatorId, PlanNodeId sourceId,
				PageSourceProvider pageSourceProvider, List<Type> types,
				Iterable<ColumnHandle> columns, PushDown pushDown) {
			this.operatorId = operatorId;
			this.sourceId = requireNonNull(sourceId, "sourceId is null");
			this.types = requireNonNull(types, "types is null");
			this.pageSourceProvider = requireNonNull(pageSourceProvider,
					"pageSourceProvider is null");
			this.columns = ImmutableList
					.copyOf(requireNonNull(columns, "columns is null"));
			this.pushDown = pushDown;
		}

		@Override
		public PlanNodeId getSourceId() {
			return sourceId;
		}

		@Override
		public List<Type> getTypes() {
			return types;
		}

		@Override
		public SourceOperator createOperator(DriverContext driverContext) {
			checkState(!closed, "Factory is already closed");
			OperatorContext operatorContext = driverContext.addOperatorContext(
					operatorId, sourceId,
					TableScanOperator.class.getSimpleName());
			return new TableScanOperator(operatorContext, sourceId,
					pageSourceProvider, types, columns, pushDown);
		}

		@Override
		public void close() {
			closed = true;
		}
	}

	private final OperatorContext operatorContext;
	private final PlanNodeId planNodeId;
	private final PageSourceProvider pageSourceProvider;
	private final List<Type> types;
	private final List<ColumnHandle> columns;
	private final LocalMemoryContext systemMemoryContext;
	private final SettableFuture<?> blocked = SettableFuture.create();

	private Split split;
	private ConnectorPageSource source;

	private PushDown pushDown;
	private boolean finished;
	Map<Symbol, Expression> projections;

	private long completedBytes;
	private long readTimeNanos;

	public TableScanOperator(OperatorContext operatorContext,
			PlanNodeId planNodeId, PageSourceProvider pageSourceProvider,
			List<Type> types, Iterable<ColumnHandle> columns) {
		this.operatorContext = requireNonNull(operatorContext,
				"operatorContext is null");
		this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
		this.types = requireNonNull(types, "types is null");
		this.pageSourceProvider = requireNonNull(pageSourceProvider,
				"pageSourceProvider is null");
		this.columns = ImmutableList
				.copyOf(requireNonNull(columns, "columns is null"));
		this.systemMemoryContext = operatorContext.getSystemMemoryContext()
				.newLocalMemoryContext();
	}

	public TableScanOperator(OperatorContext operatorContext,
			PlanNodeId planNodeId, PageSourceProvider pageSourceProvider,
			List<Type> types, Iterable<ColumnHandle> columns,
			PushDown pushDown) {
		this.operatorContext = requireNonNull(operatorContext,
				"operatorContext is null");
		this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
		this.types = requireNonNull(types, "types is null");
		this.pageSourceProvider = requireNonNull(pageSourceProvider,
				"pageSourceProvider is null");
		this.columns = ImmutableList
				.copyOf(requireNonNull(columns, "columns is null"));
		this.systemMemoryContext = operatorContext.getSystemMemoryContext()
				.newLocalMemoryContext();
		this.pushDown = pushDown;
	}

	@Override
	public OperatorContext getOperatorContext() {
		return operatorContext;
	}

	@Override
	public PlanNodeId getSourceId() {
		return planNodeId;
	}

	@Override
	public Supplier<Optional<UpdatablePageSource>> addSplit(Split split) {
		requireNonNull(split, "split is null");
		checkState(this.split == null, "Table scan split already set");

		// add filter information into split and cursor
		if (pushDown != null) {
			if (PushDownUtilities.isPushDownFilter(
					pushDown.getValue(SimpleFilterPair.class))) {
				split.getConnectorSplit().getPushDown().fromPushDown(pushDown);
			}
		}

		if (finished) {
			return Optional::empty;
		}

		this.split = split;

		Object splitInfo = split.getInfo();
		if (splitInfo != null) {
			operatorContext.setInfoSupplier(() -> splitInfo);
		}

		blocked.set(null);

		return () -> {
			if (source instanceof UpdatablePageSource) {
				return Optional.of((UpdatablePageSource) source);
			}
			return Optional.empty();
		};
	}

	@Override
	public void noMoreSplits() {
		if (split == null) {
			finished = true;
		}
		blocked.set(null);
	}

	@Override
	public List<Type> getTypes() {
		return types;
	}

	@Override
	public void close() {
		finish();
		log.info("time for getNextPage" + this.nextPageTime / 1000000);
		log.info("time for updateOpreator" + this.updateOpreator / 1000000);
		log.info("time for writeTime" + this.writeTime / 1000000);
	}

	@Override
	public void finish() {
		finished = true;
		blocked.set(null);

		if (source != null) {
			try {
				source.close();
			} catch (IOException e) {
				throw Throwables.propagate(e);
			}
			systemMemoryContext.setBytes(source.getSystemMemoryUsage());
		}
	}

	@Override
	public boolean isFinished() {
		if (!finished) {
			createSourceIfNecessary();
			finished = (source != null) && source.isFinished();
			if (source != null) {
				systemMemoryContext.setBytes(source.getSystemMemoryUsage());
			}
		}

		return finished;
	}

	@Override
	public ListenableFuture<?> isBlocked() {
		return blocked;
	}

	@Override
	public boolean needsInput() {
		return false;
	}

	@Override
	public void addInput(Page page) {
		throw new UnsupportedOperationException(
				getClass().getName() + " can not take input");
	}

	long nextPageTime = 0;

	long lastTime = 0;

	long writeTime = 0;

	long updateOpreator = 0;

	@Override
	public Page getOutput() {
		if (lastTime != 0) {
			writeTime += (System.nanoTime() - lastTime);
		}
		long start = System.nanoTime();
		createSourceIfNecessary();
		if (source == null) {
			return null;
		}

		Page page = source.getNextPage();
		nextPageTime += (System.nanoTime() - start);
		long s = System.nanoTime();
		if (page != null) {
			// assure the page is in memory before handing to another operator
			page.assureLoaded();

			// update operator stats
			long endCompletedBytes = source.getCompletedBytes();
			long endReadTimeNanos = source.getReadTimeNanos();
			operatorContext.recordGeneratedInput(
					endCompletedBytes - completedBytes, page.getPositionCount(),
					endReadTimeNanos - readTimeNanos);
			completedBytes = endCompletedBytes;
			readTimeNanos = endReadTimeNanos;
		}

		// updating system memory usage should happen after page is loaded.
		systemMemoryContext.setBytes(source.getSystemMemoryUsage());
		updateOpreator += (System.nanoTime() - s);
		lastTime = System.nanoTime();
		return page;
	}

	private void createSourceIfNecessary() {
		if ((split != null) && (source == null)) {
			source = pageSourceProvider.createPageSource(
					operatorContext.getSession(), split, columns);
		}
	}
}
