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
import java.util.List;

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
import com.facebook.presto.sql.pair.Distinct2GroupbyPair;
import com.facebook.presto.sql.pair.DistinctPair;
import com.facebook.presto.sql.pair.HashOutputSignature;
import com.facebook.presto.sql.pair.LimitPair;
import com.facebook.presto.sql.pair.OutputPair;
import com.facebook.presto.sql.pair.OutputSignature;
import com.facebook.presto.sql.pair.OutputSignature.OutputType;
import com.facebook.presto.sql.pair.SimpleFilterPair;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class DistinctQueryHandler extends AbstractQueryHandler {

	private IndexReader reader;

	private LuceneRecordCursor curser;

	protected DistinctQueryHandler(IndexReader reader,
			LuceneRecordCursor curser) {
		super(new LuceneQueryTransfer(curser));
		this.reader = reader;
		this.curser = curser;
		this.setSuccessor(new MutiDistinctQueryHandler(reader, curser));
	}

	@Override
	public WeiwoCollector handlenow(PushDown pushdown) throws IOException {
		Expression express = pushdown.getValue(SimpleFilterPair.class);
		Query filterQuery = transfer.transfer(express);
		IndexSearcher searcher = new IndexSearcher(reader);
		WeiwoCollector collector = new DistinctWeiwoCollector(pushdown);
		searcher.search(filterQuery, collector);
		return collector;
	}

	@Override
	public boolean ishandover(PushDown pushdown) {
		List<OutputSignature> masks = pushdown.getValue(DistinctPair.class);
		return masks != null && masks.size() > 1;
	}

	public class DistinctWeiwoCollector extends WeiwoTemplateCollector {

		private DocIterator docidIterator;

		Object[] row;

		SegmentWriter writer;

		final SegmentWriterFactory segment;

		long limit = -1;

		private HashOutputSignature hash;

		DocIdsList docids;

		private List<OutputSignature> combineDistinct(PushDown pushdown) {
			List<OutputSignature> masks = pushdown.getValue(DistinctPair.class);

			OutputSignature distinct2groupby = pushdown
					.getValue(Distinct2GroupbyPair.class);

			if (masks == null) {
				masks = new ArrayList<OutputSignature>();
			}

			if (distinct2groupby != null) {
				masks.add(distinct2groupby);
			}
			return masks;
		}

		public DistinctWeiwoCollector(PushDown pushdown) {
			super(DistinctQueryHandler.this.reader,
					DistinctQueryHandler.this.curser);
			List<OutputSignature> outputs = pushdown.getValue(OutputPair.class);

			row = new Object[outputs.size()];

			segment = new SegmentWriterFactory(combineDistinct(pushdown), this);

			List<OutputSignature> hashs = new ArrayList<OutputSignature>();

			for (OutputSignature output : outputs) {
				if (output.getKind() == OutputType.HASH) {
					hashs.add(output);
				}
			}

			hash = getOnlyHashSignature(hashs);

			Long value = pushdown.getValue(LimitPair.class);
			if (value != null) {
				limit = value.longValue();
			}
		}

		private HashOutputSignature getOnlyHashSignature(
				List<OutputSignature> hashs) {
			if (hashs.size() != 1) {
				throw Throwables.propagate(new IllegalArgumentException(
						"not support more than 1 hash value in single distcint query "));
			} else {
				return (HashOutputSignature) Iterators
						.getOnlyElement(hashs.iterator());
			}
		}

		@Override
		public void collect(int doc) throws IOException {
			docids.add(doc);
			// writer.hash(doc); //when not collection some bug appear
			this.collectNumPerSplit++;
		}

		private boolean isduplicated(int docId) {
			writer.hash(docId);
			return writer.write(row, docId);
		}

		@Override
		public boolean hasNext() {
			if (docidIterator == null) {
				docidIterator = docids.iterator();
			}
			return hasNextDoc();
		}

		private boolean hasNextDoc() {
			while (true) {
				long nextDoc = System.nanoTime();
				if (docidIterator.hasNext()) {
					int docId = docidIterator.next();
					this.nanoNextDocWhile += (System.nanoTime() - nextDoc);
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

		@Override
		public LeafCollector getLeafCollector(LeafReaderContext context)
				throws IOException {
			if (docids == null) {
				docids = new DocIdsList();
			}
			LeafCollector leafCollector = super.getLeafCollector(context);
			writer = segment.getSegmentWriter(context, curser, hash);
			return leafCollector;
		}

		@Override
		public Long getLongDocValues(int fieldIndex) throws IOException {
			return (long) row[fieldIndex];
		}

		@Override
		public Double getDoubleDocValues(int fieldIndex) throws IOException {
			return (double) row[fieldIndex];
		}

		@Override
		public Slice getSortedDocValues(int fieldIndex) throws IOException {
			Slice result = (Slice) row[fieldIndex];

			if (result == null) {
				return Slices.EMPTY_SLICE;
			}
			return result;
		}
	}
}