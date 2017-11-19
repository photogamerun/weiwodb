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
import org.apache.lucene.search.Query;

import com.facebook.presto.lucene.LuceneRecordCursor;
import com.facebook.presto.lucene.services.query.AbstractQueryHandler;
import com.facebook.presto.lucene.services.query.SegmentWriter;
import com.facebook.presto.lucene.services.query.WeiwoCollector;
import com.facebook.presto.lucene.services.query.WeiwoTemplateCollector;
import com.facebook.presto.lucene.services.query.parser.LuceneQueryTransfer;
import com.facebook.presto.lucene.services.query.util.DocIdsList;
import com.facebook.presto.lucene.services.query.util.DocIdsList.DocIterator;
import com.facebook.presto.spi.pushdown.PushDown;
import com.facebook.presto.sql.pair.DistinctPair;
import com.facebook.presto.sql.pair.HashOutputSignature;
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
public class MutiDistinctQueryHandler extends AbstractQueryHandler {

	private IndexReader reader;

	private LuceneRecordCursor curser;

	protected MutiDistinctQueryHandler(IndexReader reader,
			LuceneRecordCursor curser) {
		super(new LuceneQueryTransfer(curser));
		this.reader = reader;
		this.curser = curser;
	}
	@Override
	public WeiwoCollector handlenow(PushDown pushdown) throws IOException {
		Expression express = (Expression) pushdown
				.getValue(SimpleFilterPair.class);
		Query filterQuery = transfer.transfer(express);
		IndexSearcher searcher = new IndexSearcher(reader);
		WeiwoCollector collector = new MutiDistinctWeiwoCollector(pushdown);
		searcher.search(filterQuery, collector);
		return collector;
	}

	@Override
	public boolean ishandover(PushDown pushdown) {
		return false;
	}

	public class MutiDistinctWeiwoCollector extends WeiwoTemplateCollector {

		private List<HashOutputSignature> symbols;

		private List<SegmentWriter> segmentwriters;

		Map<List<SegmentWriter>, DocIdsList> leafGroupToDocID = new HashMap<List<SegmentWriter>, DocIdsList>();

		private Iterator<List<SegmentWriter>> writerIterator;

		private List<SegmentWriter> currentWriters;

		Object[] resultPergroup;

		private DocIterator docidIterator;

		private SegmentMutiWriterFactory segment;

		long limit = -1;

		public MutiDistinctWeiwoCollector(PushDown pushdown) {
			super(MutiDistinctQueryHandler.this.reader,
					MutiDistinctQueryHandler.this.curser);

			symbols = new ArrayList<HashOutputSignature>();

			List<OutputSignature> outputs = pushdown.getValue(OutputPair.class);

			for (OutputSignature output : outputs) {
				if (output.getKind() == OutputType.HASH) {
					symbols.add((HashOutputSignature) output);
				}
			}

			resultPergroup = new Object[outputs.size()];
			List<OutputSignature> distincts = pushdown
					.getValue(DistinctPair.class);
			segment = new SegmentMutiWriterFactory(distincts, this);
		}

		@Override
		public void collect(int doc) throws IOException {
			if (segmentwriters != null) {
				leafGroupToDocID.get(segmentwriters).add(doc);
			}
		}

		@Override
		public boolean hasNext() {
			if (leafGroupToDocID.isEmpty()) {
				return false;
			} else {
				if (writerIterator == null) {
					writerIterator = leafGroupToDocID.keySet().iterator();
					currentWriters = writerIterator.next();
					docidIterator = leafGroupToDocID.get(currentWriters)
							.iterator();
				}
				if (hashNextDoc() && limit != 0) {
					limit--;
					return true;
				} else {
					leafGroupToDocID.get(segmentwriters).release();
					if (writerIterator.hasNext()) {
						currentWriters = writerIterator.next();
						docidIterator = leafGroupToDocID.get(currentWriters)
								.iterator();
					} else {
						return false;
					}
					return hasNext();
				}
			}
		}

		private boolean hashNextDoc() {
			while (true) {
				if (docidIterator.hasNext()) {
					int docId = docidIterator.next();
					if (!isduplicated(docId)) {
						return true;
					} else {
						continue;
					}
				} else {
					return false;
				}
			}
		}

		private boolean isduplicated(int docId) {
			for (SegmentWriter writer : currentWriters) {
				writer.hash(docId);
				writer.write(resultPergroup, docId);
			}
			return false;
		}

		@Override
		public LeafCollector getLeafCollector(LeafReaderContext context)
				throws IOException {
			DocIdsList docids = new DocIdsList();
			LeafCollector leafCollector = super.getLeafCollector(context);
			segmentwriters = new ArrayList<SegmentWriter>();
			for (HashOutputSignature symbol : symbols) {
				SegmentWriter writer = segment.getSegmentWriter(context,
						this.curser, symbol);
				segmentwriters.add(writer);
			}
			leafGroupToDocID.put(segmentwriters, docids);
			return leafCollector;
		}

		@Override
		public Long getLongDocValues(int fieldIndex) throws IOException {
			return (long) resultPergroup[fieldIndex];
		}

		@Override
		public Double getDoubleDocValues(int fieldIndex) throws IOException {
			return (double) resultPergroup[fieldIndex];
		}

		@Override
		public Slice getSortedDocValues(int fieldIndex) throws IOException {
			Slice result = (Slice) resultPergroup[fieldIndex];
			if (result == null) {
				return Slices.EMPTY_SLICE;
			}
			return result;
		}
	}
}