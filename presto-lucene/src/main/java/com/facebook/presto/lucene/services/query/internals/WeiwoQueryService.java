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

import com.facebook.presto.lucene.LuceneRecordCursor;
import com.facebook.presto.lucene.services.query.QueryService;
import com.facebook.presto.lucene.services.query.WeiwoCollector;
import com.facebook.presto.spi.pushdown.PushDown;

public final class WeiwoQueryService implements QueryService {

	private IndexReader reader;

	LuceneRecordCursor curser;

	public WeiwoQueryService(IndexReader reader, LuceneRecordCursor curser) {
		this.reader = reader;
		this.curser = curser;
	}

	@Override
	public WeiwoCollector build(PushDown pushdown) throws IOException {
		StandardQueryHandler standhandler = new StandardQueryHandler(reader,
				curser);
		standhandler.setSuccessor(new AggregationQueryHandler(reader, curser))
				.setSuccessor(new GroupAggregationQueryHandler(reader, curser))
				.setSuccessor(new DistinctQueryHandler(reader, curser));
		return standhandler.handle(pushdown);
	}
}