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

import com.facebook.presto.lucene.LuceneRecordCursor;
import com.facebook.presto.lucene.services.query.AbstractQueryHandler;
import com.facebook.presto.lucene.services.query.WeiwoCollector;
import com.facebook.presto.lucene.services.query.WeiwoTemplateCollector;
import com.facebook.presto.lucene.services.query.parser.LuceneQueryTransfer;
import com.facebook.presto.spi.pushdown.PushDown;
import com.facebook.presto.sql.pair.DistinctPair;
import com.facebook.presto.sql.pair.GroupByPair;
import com.facebook.presto.sql.pair.SimpleFilterPair;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Throwables;
/**
 * 
 * 
 * @author peter.wei
 *
 */
class AggregationQueryHandler extends AbstractQueryHandler {

	protected IndexReader reader;

	protected LuceneRecordCursor curser;

	AggregationQueryHandler(IndexReader indexReader,
			LuceneRecordCursor curser) {
		super(new LuceneQueryTransfer(curser));
		this.reader = indexReader;
		this.curser = curser;
	}

	@Override
	public WeiwoCollector handlenow(PushDown pushdown) throws IOException {
		Expression express = (Expression) pushdown
				.getValue(SimpleFilterPair.class);
		Query luceneQuery = transfer.transfer(express);
		WeiwoCollector aggCollector = new CountCollector(luceneQuery);
		return aggCollector;
	}

	@Override
	public boolean ishandover(PushDown pushdown) {
		return hasDistinct(pushdown) || hasGroupby(pushdown);
	}

	private boolean hasGroupby(PushDown pushdown) {
		return pushdown.has(GroupByPair.class);
	}

	private boolean hasDistinct(PushDown pushdown) {
		return pushdown.has(DistinctPair.class);
	}

	private class CountCollector extends WeiwoTemplateCollector {

		private int totalCount;

		private int maxSize = 1;

		public CountCollector(Query luceneQuery) {
			super(AggregationQueryHandler.this.reader,
					AggregationQueryHandler.this.curser);
			IndexSearcher searcher = new IndexSearcher(reader);
			try {
				totalCount = searcher.count(luceneQuery);
			} catch (IOException e) {
				throw Throwables.propagate(e);
			}
		}

		@Override
		public void collect(int doc) throws IOException {
		}

		@Override
		public Long getLongDocValues(int fieldIndex) throws IOException {
			return (long) totalCount;
		}

		@Override
		public long size() {
			return 1;
		}

		@Override
		public boolean hasNext() {
			return next() == 0;
		}

		@Override
		public Integer next() {
			maxSize--;
			return maxSize;
		}
	}
}