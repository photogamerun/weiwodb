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

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.facebook.presto.lucene.hive.metastore.HiveType;
import com.facebook.presto.lucene.index.CacheIndexManager;
import com.facebook.presto.lucene.services.writer.WeiwoDBSplitWriterManager;
import com.facebook.presto.lucene.util.LuceneColumnUtil;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.pair.OutputPair;
import com.facebook.presto.sql.pair.OutputSignature;
import com.facebook.presto.sql.pair.OutputSignature.OutputType;

public class LuceneRecordSet implements RecordSet {

	private List<LuceneColumnHandle> columnHandles;
	private List<Type> columnTypes;
	private final LuceneSplit split;
	private final TypeManager typeManager;
	private final CacheIndexManager indexManager;
	private final WeiwoDBSplitWriterManager writerManager;
	private Map<String, String> name2LuceneType = new HashMap<String, String>();

	public LuceneRecordSet(LuceneSplit split,
			List<LuceneColumnHandle> columnHandles, TypeManager typeManager,
			CacheIndexManager indexManager,
			WeiwoDBSplitWriterManager writerManager) {
		this.split = requireNonNull(split, "split is null");
		this.typeManager = requireNonNull(typeManager, "typeManager is null");
		this.indexManager = requireNonNull(indexManager,
				"indexManager is null");
		this.writerManager = requireNonNull(writerManager,
				"indexManager is null");
		this.columnHandles = requireNonNull(columnHandles,
				"column handles is null");
		for (LuceneColumnHandle columnHandle : columnHandles) {
			name2LuceneType.put(columnHandle.getColumnName(),
					columnHandle.getLuceneType());
		}

		List<OutputSignature> outputSymbols = split.getPushDown()
				.getValue(OutputPair.class);

		LuceneColumnHandle[] columnHandlesBuilder = new LuceneColumnHandle[outputSymbols
				.size()];

		Type[] types = new Type[outputSymbols.size()];

		for (OutputSignature outputSymbol : outputSymbols) {
			if (outputSymbol.getKind() == OutputType.FIELD) {
				for (LuceneColumnHandle columnHandle : columnHandles) {
					name2LuceneType.put(columnHandle.getColumnName(),
							columnHandle.getLuceneType());
					if (columnHandle.getColumnName()
							.equalsIgnoreCase(outputSymbol.getName())) {
						LuceneColumnHandle newColumn = new LuceneColumnHandle(
								columnHandle.getConnectorId(),
								columnHandle.getColumnName(),
								columnHandle.getColumnType(),
								outputSymbol.getChannel(),
								columnHandle.getTypeSignature(),
								columnHandle.getLuceneType());
						columnHandlesBuilder[outputSymbol
								.getChannel()] = newColumn;
						types[outputSymbol.getChannel()] = typeManager
								.getType(newColumn.getTypeSignature());
					}
				}
			}

			if (outputSymbol.getKind() == OutputType.HASH) {
				LuceneHashColumnHandle hashColumn = new LuceneHashColumnHandle(
						split.getConnectorId(), LuceneColumnUtil.HASHCODE,
						HiveType.HIVE_LONG, outputSymbol.getChannel(),
						HiveType.HIVE_LONG.getTypeSignature(),
						WeiwoDBType.LONG);
				columnHandlesBuilder[outputSymbol.getChannel()] = hashColumn;
				types[outputSymbol.getChannel()] = BigintType.BIGINT;
				name2LuceneType.put(hashColumn.getColumnName(),
						hashColumn.getLuceneType());
			}

			if (outputSymbol.getKind() == OutputType.AGGREAT) {
				LuceneAggColumnHandle aggColumn = new LuceneAggColumnHandle(
						split.getConnectorId(), outputSymbol.getName(),
						HiveType.HIVE_LONG, outputSymbol.getChannel(),
						HiveType.HIVE_LONG.getTypeSignature(), "long");
				columnHandlesBuilder[outputSymbol.getChannel()] = aggColumn;
				types[outputSymbol.getChannel()] = BigintType.BIGINT;
				name2LuceneType.put(aggColumn.getColumnName(),
						aggColumn.getLuceneType());
			}
		}
		// 填充非分组非聚合列

		List<Type> tempTypeList = Arrays.asList(types);

		List<LuceneColumnHandle> tempColumnHandle = Arrays
				.asList(columnHandlesBuilder);

		this.columnTypes = tempTypeList;

		this.columnHandles = tempColumnHandle;

	}

	@Override
	public List<Type> getColumnTypes() {
		return columnTypes;
	}

	@Override
	public RecordCursor cursor() {
		return new LuceneRecordCursor(split, columnHandles, indexManager,
				writerManager, typeManager, name2LuceneType, getColumnTypes());
	}
}
