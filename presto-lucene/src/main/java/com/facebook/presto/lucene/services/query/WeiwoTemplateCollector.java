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
package com.facebook.presto.lucene.services.query;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;

import com.facebook.presto.lucene.LuceneRecordCursor;
import com.facebook.presto.lucene.services.query.util.DocIdsList;
import com.facebook.presto.lucene.services.query.util.DocIdsList.DocIterator;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public abstract class WeiwoTemplateCollector extends WeiwoCollectorAdapter {

	final int LENGTH = 1024 * 1024;

	protected int currentDocId = 0;

	protected int docBase;

	protected int numDocsComplete = 0;

	protected IndexReader reader;

	private int count;

	protected LuceneRecordCursor curser;

	private DocIdsList docIds;

	private DocIterator docIterator;

	public WeiwoTemplateCollector(IndexReader reader,
			LuceneRecordCursor curser) {
		this.reader = reader;
		this.curser = curser;
		this.docIds = new DocIdsList();
	}

	@Override
	public boolean needsScores() {
		return false;
	}

	@Override
	public void collect(int doc) throws IOException {
		currentDocId = docBase + doc;
		docIds.add(currentDocId);
		count++;
	}

	@Override
	public LeafCollector getLeafCollector(LeafReaderContext context)
			throws IOException {
		docBase = context.docBase;
		return this;
	}

	@Override
	public Long getLongDocValues(int fieldIndex) throws IOException {
		List<LeafReaderContext> leaves = reader.leaves();
		int i = ReaderUtil.subIndex(currentDocId, leaves);
		LeafReaderContext currentLeaf = leaves.get(i);
		int segmentDocId = currentDocId - currentLeaf.docBase;
		return DocValues.getNumeric(currentLeaf.reader(),
				curser.getColumnName(fieldIndex)).get(segmentDocId);
	}

	@Override
	public Double getDoubleDocValues(int fieldIndex) throws IOException {
		long longValue = getLongDocValues(fieldIndex);
		return Double.longBitsToDouble(longValue);
	}

	@Override
	public Slice getSortedDocValues(int fieldIndex) throws IOException {
		List<LeafReaderContext> leaves = reader.leaves();
		int i = ReaderUtil.subIndex(currentDocId, leaves);
		LeafReaderContext currentLeaf = leaves.get(i);
		int segmentDocId = currentDocId - currentLeaf.docBase;
		if ("text".equalsIgnoreCase(curser.getColumnLuceneType(fieldIndex))) {
			String value = currentLeaf.reader().document(segmentDocId)
					.get(curser.getColumnName(fieldIndex));
			return value == null ? Slices.EMPTY_SLICE : Slices.utf8Slice(value);
		} else {
			SortedDocValues values = DocValues.getSorted(currentLeaf.reader(),
					curser.getColumnName(fieldIndex));
			int ord = values.getOrd(segmentDocId);
			return ord == -1
					? Slices.EMPTY_SLICE
					: Slices.utf8Slice(values.lookupOrd(ord).utf8ToString());
		}
	}

	@Override
	public void setScorer(Scorer arg0) throws IOException {

	}

	@Override
	public boolean hasNext() {
		if (docIterator == null) {
			docIterator = docIds.iterator();
		}
		if (docIterator.hasNext()) {
			currentDocId = next();
			return true;
		} else {
			return false;
		}
	}

	@Override
	public Integer next() {
		numDocsComplete++;
		return docIterator.next();
	}

	public int numDocsComplete() {
		return numDocsComplete;
	}

	@Override
	public long size() {
		return count;
	}

	@Override
	public long getTotalBytes(long avgBytesPerDoc) {
		return size() * avgBytesPerDoc;
	}

	@Override
	public long getCompleteBytes(long avgBytesPerDoc) {
		return numDocsComplete() * avgBytesPerDoc;
	}
}
