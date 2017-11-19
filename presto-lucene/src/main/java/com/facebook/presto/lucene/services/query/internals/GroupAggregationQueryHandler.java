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
package com.facebook.presto.lucene.services.query.internals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import com.facebook.presto.lucene.LuceneRecordCursor;
import com.facebook.presto.lucene.services.query.AbstractQueryHandler;
import com.facebook.presto.lucene.services.query.Aggregation;
import com.facebook.presto.lucene.services.query.SegmentWriter;
import com.facebook.presto.lucene.services.query.WeiwoCollector;
import com.facebook.presto.lucene.services.query.WeiwoTemplateCollector;
import com.facebook.presto.lucene.services.query.parser.LuceneQueryTransfer;
import com.facebook.presto.spi.pushdown.PushDown;
import com.facebook.presto.sql.pair.AggregationPair;
import com.facebook.presto.sql.pair.DistinctPair;
import com.facebook.presto.sql.pair.GroupByPair;
import com.facebook.presto.sql.pair.HashOutputSignature;
import com.facebook.presto.sql.pair.LimitPair;
import com.facebook.presto.sql.pair.OutputPair;
import com.facebook.presto.sql.pair.OutputSignature;
import com.facebook.presto.sql.pair.OutputSignature.OutputType;
import com.facebook.presto.sql.pair.SimpleFilterPair;
import com.facebook.presto.sql.tree.Expression;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

/**
 * 
 * @author peter.wei
 *
 */
class GroupAggregationQueryHandler extends AbstractQueryHandler {

	private IndexReader reader;

	private LuceneRecordCursor curser;

	GroupAggregationQueryHandler(IndexReader reader,
			LuceneRecordCursor curser) {
		super(new LuceneQueryTransfer(curser));
		this.reader = reader;
		this.curser = curser;
	}

	@Override
	public WeiwoCollector handlenow(PushDown pushdown) throws IOException {
		return handleGroup(pushdown);
	}

	private WeiwoCollector handleGroup(PushDown pushdown) throws IOException {
		Expression express = (Expression) pushdown
				.getValue(SimpleFilterPair.class);

		Query filterQuery = transfer.transfer(express);
		if (filterQuery instanceof MatchAllDocsQuery) {

		}
		IndexSearcher searcher = new IndexSearcher(reader);
		WeiwoCollector collector = new GroupByWeiwoCollector(pushdown);
		searcher.search(filterQuery, collector);
		return collector;
	}

	@Override
	public boolean ishandover(PushDown pushdown) {
		return (!hasAggregation(pushdown))
				&& (hasDistinct(pushdown) || hasGroupby(pushdown));
	}

	private boolean hasDistinct(PushDown pushdown) {
		return pushdown.has(DistinctPair.class);
	}

	private boolean hasGroupby(PushDown pushdown) {
		return pushdown.has(GroupByPair.class);
	}

	private boolean hasAggregation(PushDown pushdown) {
		return pushdown.has(AggregationPair.class);
	}

	private class GroupByWeiwoCollector extends WeiwoTemplateCollector {

		Map<Long, Object[]> groupResults = new HashMap<Long, Object[]>();

		List<Aggregation> aggregations = new ArrayList<>();

		SegmentWriter segmentWriter;

		Object[] currentGroupResult;

		private HashOutputSignature groupby;

		private Iterator<Object[]> groupResultIterator;

		int blocks;

		long limit = -1;

		final SegmentWriterFactory segment;

		public GroupByWeiwoCollector(PushDown pushdown) {
			super(GroupAggregationQueryHandler.this.reader,
					GroupAggregationQueryHandler.this.curser);

			List<OutputSignature> outputPairs = pushdown
					.getValue(OutputPair.class);

			for (OutputSignature outputPair : outputPairs) {
				if (outputPair.getKind() == OutputType.AGGREAT) {
					aggregations.add(AggregationMediator.mediate(outputPair));
				} else if (outputPair.getKind() == OutputType.HASH) {
					groupby = (HashOutputSignature) outputPair;
				}
			}

			blocks = outputPairs.size();

			Object value = pushdown.getValue(LimitPair.class);

			limit = value == null ? Long.MAX_VALUE : (long) value;

			segment = new SegmentWriterFactory(this);

		}

		@Override
		public void collect(int doc) throws IOException {

			long currentHashCode = segmentWriter.hash(doc);;

			Object[] resultPergroup = groupResults.get(currentHashCode);

			if (resultPergroup == null) {
				resultPergroup = new Object[blocks];

				resultPergroup = segmentWriter.write(resultPergroup);

				groupResults.put(currentHashCode, resultPergroup);
			}

			for (Aggregation aggregation : aggregations) {
				aggregation.aggregate(resultPergroup);
			}
		}

		@Override
		public LeafCollector getLeafCollector(LeafReaderContext context)
				throws IOException {
			LeafCollector collector = super.getLeafCollector(context);
			segmentWriter = segment.getSegmentWriter(context, curser, groupby);
			return collector;
		}

		@Override
		public Long getLongDocValues(int fieldIndex) throws IOException {
			return (long) currentGroupResult[fieldIndex];
		}

		@Override
		public Double getDoubleDocValues(int fieldIndex) throws IOException {
			return (double) currentGroupResult[fieldIndex];
		}

		@Override
		public Slice getSortedDocValues(int fieldIndex) throws IOException {
			Slice result = (Slice) currentGroupResult[fieldIndex];
			if (result == null) {
				return Slices.EMPTY_SLICE;
			}
			return result;
		}

		@Override
		public boolean hasNext() {
			if (groupResultIterator == null) {
				groupResultIterator = groupResults.values().iterator();
			}
			if (groupResultIterator.hasNext() && limit-- > 0) {
				currentGroupResult = groupResultIterator.next();
				return true;
			} else {
				return false;
			}
		}

		@Override
		public long size() {
			return groupResults.keySet().size();
		}
	}
}