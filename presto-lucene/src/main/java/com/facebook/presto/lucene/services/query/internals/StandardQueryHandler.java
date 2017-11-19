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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;

import com.facebook.presto.lucene.LuceneRecordCursor;
import com.facebook.presto.lucene.services.query.AbstractQueryHandler;
import com.facebook.presto.lucene.services.query.WeiwoCollector;
import com.facebook.presto.lucene.services.query.WeiwoTemplateCollector;
import com.facebook.presto.lucene.services.query.parser.LuceneQueryTransfer;
import com.facebook.presto.spi.pushdown.PushDown;
import com.facebook.presto.sql.pair.AggregationPair;
import com.facebook.presto.sql.pair.DistinctPair;
import com.facebook.presto.sql.pair.GroupByPair;
import com.facebook.presto.sql.pair.LimitPair;
import com.facebook.presto.sql.pair.SimpleFilterPair;
import com.facebook.presto.sql.tree.Expression;
/**
 * 
 * @author peter.wei
 *
 */
class StandardQueryHandler extends AbstractQueryHandler {

	private IndexReader reader;

	private LuceneRecordCursor curser;

	StandardQueryHandler(IndexReader reader, LuceneRecordCursor curser) {
		super(new LuceneQueryTransfer(curser));
		this.reader = reader;
		this.curser = curser;
	}

	@Override
	public WeiwoCollector handlenow(PushDown pushdown) throws IOException {
		Expression express = pushdown.getValue(SimpleFilterPair.class);
		Query filterQuery = transfer.transfer(express);
		IndexSearcher searcher = new IndexSearcher(reader);
		SimpleCollector collector = new SimpleCollector(pushdown);
		searcher.search(filterQuery, collector);
		return collector;
	}

	@Override
	public boolean ishandover(PushDown pushdown) {
		return hasGroupby(pushdown) || hasAggregation(pushdown)
				|| hasDistinct(pushdown);
	}

	private boolean hasGroupby(PushDown pushdown) {
		return pushdown.has(GroupByPair.class);
	}

	private boolean hasDistinct(PushDown pushdown) {
		return pushdown.has(DistinctPair.class);
	}

	private boolean hasAggregation(PushDown pushdown) {
		return pushdown.has(AggregationPair.class);
	}

	public class SimpleCollector extends WeiwoTemplateCollector {

		private long limit = -1;

		public SimpleCollector(PushDown pushdown) {
			super(StandardQueryHandler.this.reader,
					StandardQueryHandler.this.curser);
			Long value = pushdown.getValue(LimitPair.class);
			this.limit = value == null ? -1 : (long) value;
		}

		@Override
		public void collect(int doc) throws IOException {
			if (limit == -1) {
				super.collect(doc);
			} else {
				if (size() < limit) {
					super.collect(doc);
				}
			}
		}

		@Override
		public void setScorer(Scorer scorer) throws IOException {
		}

		@Override
		public boolean needsScores() {
			return false;
		}
	}
}