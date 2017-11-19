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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;

import io.airlift.slice.Slice;
/**
 * 
 * 缺省适配器
 * 
 * @author peter.wei
 *
 */
public abstract class WeiwoCollectorAdapter implements WeiwoCollector {

	public long nanoHashtime;

	public long nanoNextDocWhile;

	public long nanoCombinHashTime;

	public long nanoCalHashTime;

	public long nanoHasNextDoc;

	public long nanoNextSegement;

	public long nanoPutRecord;

	public long nanoGetLuceneDoc;

	public long nanoGetOrder;

	public long nanoOrder2HashGet;

	public long nanoOrder2HashPuty;

	public long nanoToSlice;

	public long collectNumPerSplit;

	@Override
	public LeafCollector getLeafCollector(LeafReaderContext arg0)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean needsScores() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setScorer(Scorer arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void collect(int arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Integer next() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getDoubleDocValues(int fieldIndex) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long getLongDocValues(int fieldIndex) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Slice getSortedDocValues(int fieldIndex) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getTotalBytes(long avgBytesPerDoc) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getCompleteBytes(long avgBytesPerDoc) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}
}