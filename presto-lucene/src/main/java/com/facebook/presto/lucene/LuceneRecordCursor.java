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

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;

import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.lucene.base.IndexInfo;
import com.facebook.presto.lucene.index.CacheIndexManager;
import com.facebook.presto.lucene.index.WeiwoIndexReader;
import com.facebook.presto.lucene.services.query.WeiwoCollector;
import com.facebook.presto.lucene.services.query.WeiwoCollectorAdapter;
import com.facebook.presto.lucene.services.query.internals.WeiwoQueryService;
import com.facebook.presto.lucene.services.writer.WeiwoDBSplitWriter;
import com.facebook.presto.lucene.services.writer.WeiwoDBSplitWriterManager;
import com.facebook.presto.lucene.util.LuceneColumnUtil;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class LuceneRecordCursor implements RecordCursor {

	private static final Logger log = Logger.get(LuceneRecordCursor.class);

	private int[] fieldToColumnIndex;
	private Map<Integer, LuceneColumnHandle> fieldColumnHandles;

	private TypeManager typeManager;
	private LuceneSplit split;
	private CacheIndexManager indexManager;
	private WeiwoDBSplitWriterManager writerManager;
	private IndexReader reader;
	private List<WeiwoIndexReader> wReader;
	private long totalBytes;
	private int partitionKeyField = -1;
	private Slice partition;
	private Map<String, Type> nameToType = new HashMap<>();
	private Map<String, String> nameToLuceneType = new HashMap<>();
	private List<Type> types;

	WeiwoDBSplitWriter writer;
	WeiwoCollector collector;
	private String info;

	private List<LuceneColumnHandle> columnHandles;

	private long start;

	private long collectTime;

	private long nanoGetSlice;

	private long validType;

	private long nanoHashNext;

	private Type[] typeArray;

	private String[] columnNameArray;

	private String[] luceneTypeArray;

	// for testing
	LuceneRecordCursor() {
	}

	public LuceneRecordCursor(LuceneSplit split,
			List<LuceneColumnHandle> columnHandles,
			CacheIndexManager indexManager,
			WeiwoDBSplitWriterManager writerManager, TypeManager typeManager,
			Map<String, String> nameToLuceneType, List<Type> types) {
		start = System.currentTimeMillis();
		this.typeManager = requireNonNull(typeManager, "typeManager is null");
		this.split = requireNonNull(split, "split is null");
		this.info = JSONObject.toJSONString(split.getIndexs());
		this.columnHandles = requireNonNull(columnHandles,
				"columnHandles is null");
		this.fieldToColumnIndex = new int[columnHandles.size()];
		this.fieldColumnHandles = new HashMap<>();
		this.typeArray = new Type[columnHandles.size()];
		this.columnNameArray = new String[columnHandles.size()];
		this.luceneTypeArray = new String[columnHandles.size()];
		this.types = requireNonNull(types, "types is null");
		for (int i = 0; i < columnHandles.size(); i++) {
			LuceneColumnHandle columnHandle = columnHandles.get(i);
			String columnName = columnHandle.getColumnName();
			this.fieldToColumnIndex[i] = columnHandle.getHiveColumnIndex();
			this.fieldColumnHandles.put(columnHandle.getHiveColumnIndex(),
					columnHandle);
			if (LuceneColumnUtil.isPartitionKey(columnName)) {
				partitionKeyField = i;
				partition = Slices
						.utf8Slice(split.getIndexs().get(0).getPartition());
			}
			nameToType.put(columnName,
					typeManager.getType(columnHandle.getTypeName()));
		}
		this.nameToLuceneType = nameToLuceneType;
		this.indexManager = requireNonNull(indexManager,
				"indexManager is null");
		this.writerManager = requireNonNull(writerManager,
				"indexManager is null");
		try {
			if (split.getIsWriter()) {
				IndexInfo index = split.getIndexs().get(0);
				writer = this.writerManager.getSplitWriterByKey(
						index.getSchema(), index.getTable(),
						index.getPartition(), index.getId());
				this.reader = writer.getReader();
				totalBytes = writer.getTotalSize();
			} else {
				List<WeiwoIndexReader> rab = this.indexManager
						.getIndexReader(split);
				wReader = rab;
				IndexReader[] readers = new IndexReader[rab.size()];
				long total = 0;
				for (int i = 0; i < rab.size(); i++) {
					WeiwoIndexReader wr = rab.get(i);
					total = total + wr.getTotalBytes();
					// inc WeiwoIndexReader ref
					readers[i] = wr.getReader();
				}
				totalBytes = total;
				// dec ref because indexManager.getIndexReader inc the ref
				for (WeiwoIndexReader r : wReader) {
					r.decRef();
				}

				this.reader = new MultiReader(readers, false);
			}

			WeiwoQueryService weiwoQuery = new WeiwoQueryService(reader, this);
			long start = System.currentTimeMillis();
			collector = weiwoQuery.build(split.getPushDown());
			collectTime = System.currentTimeMillis() - start;
		} catch (Throwable e) {
			try {
				if (split.getIsWriter() && writer != null && reader != null) {
					writer.returnReader(reader);
				} else {
					if (wReader != null) {
						for (WeiwoIndexReader r : wReader) {
							r.decRef();
						}
					}
					if (reader != null) {
						reader.close();
					}
				}
			} catch (Exception e1) {
				log.error(e1, "Error when close.");
			}
			log.error(e, "Create LuceneRecordCursor Error.");
			throw Throwables.propagate(e);
		}
		log.info("time for initial record curser "
				+ (System.currentTimeMillis() - start));
	}

	public List<Type> getTypes() {
		return types;
	}

	@Override
	public long getTotalBytes() {
		return totalBytes;
	}

	@Override
	public long getCompletedBytes() {
		return collector.getCompleteBytes(totalBytes);
	}

	@Override
	public long getReadTimeNanos() {
		return 0;
	}

	public Type getType(String field) {
		return nameToType.get(field);
	}

	public String getLuceneType(String field) {
		return nameToLuceneType.get(field);
	}

	@Override
	public Type getType(int field) {
		Type type = typeArray[field];
		if (type == null) {
			int index = fieldToColumnIndex[field];
			LuceneColumnHandle column = fieldColumnHandles.get(index);
			type = typeManager.getType(column.getTypeName());
			return typeArray[field] = type;
		} else {
			return type;
		}
	}

	public String getColumnName(int field) {
		String columnName = columnNameArray[field];
		if (columnName == null) {
			int index = fieldToColumnIndex[field];
			LuceneColumnHandle column = fieldColumnHandles.get(index);
			columnName = column.getColumnName();
			return columnNameArray[field] = columnName;
		} else {
			return columnName;
		}
	}

	public String getColumnLuceneType(int field) {
		String luceneType = luceneTypeArray[field];
		if (luceneType == null) {
			int index = fieldToColumnIndex[field];
			LuceneColumnHandle column = fieldColumnHandles.get(index);
			luceneType = column.getLuceneType();
			return luceneTypeArray[field] = luceneType;
		} else {
			return luceneType;
		}
	}

	public List<LuceneColumnHandle> getColumns() {
		return columnHandles;
	}

	@Override
	public boolean advanceNextPosition() {
		long start = System.nanoTime();
		boolean hasNext = collector.hasNext();
		nanoHashNext += (System.nanoTime() - start);
		return hasNext;
	}

	@Override
	public boolean getBoolean(int field) {
		return false;
	}

	@Override
	public long getLong(int field) {
		ValidLongType(field);
		try {
			Long result = collector.getLongDocValues(field);
			return result == null ? 0 : result;
		} catch (IOException e) {
			log.error(e, "LuceneRecordCursor getLong Error.");
			throw Throwables.propagate(e);
		}
	}

	private void ValidLongType(int field) {
		String luceneType = getColumnLuceneType(field);
		Type type = getType(field);
		if (!type.equals(BIGINT) && !type.equals(INTEGER)
				&& !WeiwoDBType.LONG.equalsIgnoreCase(luceneType)
				&& !WeiwoDBType.INT.equalsIgnoreCase(luceneType)
				&& !WeiwoDBType.FLOAT.equalsIgnoreCase(luceneType)) {
			throw new IllegalArgumentException(
					format("Expected field to be %s, actual %s. LuceneType to be %s, actual %s (field %s)",
							BIGINT, type, WeiwoDBType.LONG,
							getColumnLuceneType(field), field));
		}
	}

	@Override
	public double getDouble(int field) {
		validDoubleType(field);
		try {
			Double result = collector.getDoubleDocValues(field);
			return result == null ? 0.0D : result;
		} catch (IOException e) {
			log.error(e, "LuceneRecordCursor getDouble Error.");
			throw Throwables.propagate(e);
		}
	}

	private void validDoubleType(int field) {
		Type type = getType(field);
		if (!type.equals(DOUBLE) && !WeiwoDBType.DOUBLE
				.equalsIgnoreCase(getColumnLuceneType(field))) {
			throw new IllegalArgumentException(
					format("Expected field to be %s, actual %s. LuceneType to be %s, actual %s (field %s)",
							DOUBLE, type, WeiwoDBType.DOUBLE,
							getColumnLuceneType(field), field));
		}
	}

	@Override
	public Slice getSlice(int field) {
		long start = System.nanoTime();
		validateType(field, Slice.class);
		validType += (System.nanoTime() - start);
		try {
			if (field == partitionKeyField) {
				return partition;
			} else {
				return collector.getSortedDocValues(field);
			}
		} catch (IOException e) {
			log.error(e, "LuceneRecordCursor getSlice Error.");
			throw Throwables.propagate(e);
		} finally {
			nanoGetSlice += (System.nanoTime() - start);
		}
	}

	private void validateType(int fieldId, Class<?> type) {
		if (!getType(fieldId).getJavaType().equals(type)) {
			// we don't use Preconditions.checkArgument because it requires
			// boxing fieldId, which affects inner loop performance
			throw new IllegalArgumentException(String.format(
					"Expected field to be %s, actual %s (field %s)", type,
					getType(fieldId), fieldId));
		}
	}

	@Override
	public Object getObject(int field) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isNull(int field) {
		return false;
	}

	@Override
	public void close() {
		try {
			WeiwoCollectorAdapter adapter = (WeiwoCollectorAdapter) collector;

			log.info("time for get docValue "
					+ adapter.nanoGetLuceneDoc / 1000000);

			log.info("time for get combinHash  "
					+ adapter.nanoCombinHashTime / 1000000);

			log.info("time for get calHash(CityHash)  "
					+ adapter.nanoCalHashTime / 1000000);

			log.info("time for get hashValue in duplicate for one column  "
					+ adapter.nanoHashtime / 1000000);

			log.info("time for put record to object array  "
					+ adapter.nanoPutRecord / 1000000);

			log.info("time for to slice  " + adapter.nanoToSlice / 1000000);

			log.info("time for to order2hashGet  "
					+ adapter.nanoOrder2HashGet / 1000000);

			log.info("time for to order2hashPut  "
					+ adapter.nanoOrder2HashPuty / 1000000);

			log.info("time for to nanoHasNextDoc  "
					+ adapter.nanoHasNextDoc / 1000000);

			log.info("time for to skip to next segment  "
					+ adapter.nanoNextSegement / 1000000);

			log.info("time for to nanoHasNextDocWhile  "
					+ adapter.nanoNextDocWhile / 1000000);

			log.info("time for to getOrder  " + adapter.nanoGetOrder / 1000000);

			log.info("time for get nanoHasNext " + (nanoHashNext / 1000000));
			log.info("time for get nanoGetRecord " + (nanoGetSlice / 1000000));
			log.info("time for collect" + adapter.collectNumPerSplit
					+ " docIDs " + collectTime);
			log.info("time for validType " + (validType / 1000000));
			if (split.getIsWriter()) {
				writer.returnReader(reader);
			} else {
				reader.close();
				for (WeiwoIndexReader r : wReader) {
					r.decRef();
				}
			}
			log.info("total time elapse "
					+ (System.currentTimeMillis() - start));
		} catch (IOException e) {
			log.error(e, "LuceneRecordCursor close reader Error.");
			throw Throwables.propagate(e);
		}
	}

	@Override
	public String toString() {
		return info;
	}
}
