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
package com.facebook.presto.lucene;

import static com.facebook.presto.lucene.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.List;

import javax.inject.Inject;

import com.facebook.presto.lucene.index.CacheIndexManager;
import com.facebook.presto.lucene.services.writer.WeiwoDBSplitWriterManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

public class LuceneRecordSetProvider implements ConnectorRecordSetProvider {

	private final String connectorId;

	private final TypeManager typeManager;

	private final CacheIndexManager indexManager;
	private final WeiwoDBSplitWriterManager writerManager;

	@Inject
	public LuceneRecordSetProvider(LuceneConnectorId connectorId,
			TypeManager typeManager, CacheIndexManager indexManager,
			WeiwoDBSplitWriterManager writerManager) {
		this.connectorId = requireNonNull(connectorId, "connectorId is null")
				.toString();
		this.typeManager = requireNonNull(typeManager, "typeManager is null");
		this.indexManager = requireNonNull(indexManager,
				"indexManager is null");
		this.writerManager = requireNonNull(writerManager,
				"indexManager is null");
	}

	@Override
	public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle,
			ConnectorSession session, ConnectorSplit split,
			List<? extends ColumnHandle> columns) {
		requireNonNull(split, "split is null");
		LuceneSplit exampleSplit = checkType(split, LuceneSplit.class, "split");
		checkArgument(exampleSplit.getConnectorId().equals(connectorId),
				"split is not for this connector");

		ImmutableList.Builder<LuceneColumnHandle> handles = ImmutableList
				.builder();
		for (ColumnHandle handle : columns) {
			handles.add(checkType(handle, LuceneColumnHandle.class, "handle"));
		}

		return new LuceneRecordSet(exampleSplit, handles.build(), typeManager,
				indexManager, writerManager);
	}

}
