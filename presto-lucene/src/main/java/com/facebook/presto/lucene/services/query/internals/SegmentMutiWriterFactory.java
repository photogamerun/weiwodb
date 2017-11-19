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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.weakref.jmx.internal.guava.base.Throwables;
import org.weakref.jmx.internal.guava.collect.Iterators;

import com.facebook.presto.lucene.LuceneRecordCursor;
import com.facebook.presto.lucene.services.query.HashFunction.LongHashFunction;
import com.facebook.presto.lucene.services.query.HashFunction.TextHashFunction;
import com.facebook.presto.lucene.services.query.HashFunction.VarcharHashFunction;
import com.facebook.presto.lucene.services.query.SegmentWriter;
import com.facebook.presto.lucene.services.query.WeiwoCollector;
import com.facebook.presto.lucene.services.query.WeiwoCollectorAdapter;
import com.facebook.presto.lucene.services.query.WeiwoSegmentDocValues;
import com.facebook.presto.lucene.services.query.WeiwoSegmentDocValues.Field;
import com.facebook.presto.lucene.util.LightWeightIntMap;
import com.facebook.presto.lucene.util.LightWeightLongMap;
import com.facebook.presto.operator.scalar.CombineHashFunction;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.FloatType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.pair.HashOutputSignature;
import com.facebook.presto.sql.pair.OutputSignature;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class SegmentMutiWriterFactory {

	private Collection<OutputSignature> distinctFields;

	private WeiwoCollectorAdapter adapter;

	public SegmentMutiWriterFactory(WeiwoCollector adapter) {
		this(new ArrayList<OutputSignature>(), adapter);

	}

	public SegmentMutiWriterFactory(Collection<OutputSignature> distinctFields,
			WeiwoCollector adapter) {
		this.adapter = (WeiwoCollectorAdapter) adapter;
		this.distinctFields = distinctFields;
	}

	private boolean isDistinct(String groupColumnName) {
		for (OutputSignature distinct : distinctFields) {
			if (Objects.equals(distinct.getName(), groupColumnName)) {
				return true;
			}
		}
		return false;
	}

	SegmentWriter getSegmentWriter(LeafReaderContext context,
			LuceneRecordCursor cursor, HashOutputSignature groupby)
			throws IOException {
		LeafReader leafReader = context.reader();
		List<Integer> channels = groupby.getDependences();
		boolean isCombineHash = channels.size() > 1;
		List<WeiwoSegmentDocValues> weiwoDocValues = new ArrayList<WeiwoSegmentDocValues>();
		for (Integer channel : channels) {
			String columnName = cursor.getColumnName(channel);
			Type type = cursor.getType(channel);
			Field field = new Field(columnName, isDistinct(columnName));
			if (Objects.equals(BigintType.BIGINT, type)
					|| Objects.equals(IntegerType.INTEGER, type)) {
				weiwoDocValues.add(new LongWeiwoDocValues(leafReader, field,
						isCombineHash, channel));
			} else if (Objects.equals(DoubleType.DOUBLE, type)) {
				weiwoDocValues.add(new DoubleWeiwoDocValues(leafReader, field,
						isCombineHash, channel));
			} else if (Objects.equals(RealType.REAL, type)
					|| Objects.equals(FloatType.FLOAT, type)) {
				weiwoDocValues.add(new FloatWeiwoDocValues(leafReader, field,
						isCombineHash, channel));
			} else if (Objects.equals(TimestampType.TIMESTAMP, type)) {
				weiwoDocValues.add(new TimeStampWeiwoDocValues(leafReader,
						field, isCombineHash, channel));
			} else {
				if (Objects.equals("text", cursor.getLuceneType(columnName))) {
					weiwoDocValues.add(new TextWeiwoDocValues(columnName,
							leafReader, channel));
				} else {
					weiwoDocValues.add(new SortedWeiwoDocValues(leafReader,
							field, channel));
				}
			}
		}
		if (isCombineHash) {
			return new CombineSegmentWriter(weiwoDocValues,
					groupby.getChannel());
		} else {
			return new SingleSegmentWriter(
					Iterators.getOnlyElement(weiwoDocValues.iterator()),
					groupby.getChannel());
		}
	}

	class LongWeiwoDocValues
			extends
				WeiwoSegmentDocValues<NumericDocValues, Long, Long> {

		LightWeightLongMap<Long> ord2Hash;

		public LongWeiwoDocValues(LeafReader leafReader, Field groupColumnNames,
				boolean isMutiple, Integer channels) throws IOException {
			super(DocValues.getNumeric(leafReader,
					groupColumnNames.getColumnName()),
					new LongHashFunction(isMutiple), groupColumnNames,
					channels);
			ord2Hash = new LightWeightLongMap<>();
		}

		@Override
		public long hash(int doc) {
			Long longvalue = getDocGroupValue(doc);
			Long hash = ord2Hash.get(longvalue);
			if (hash != null) {
				return hash;
			} else {
				try {
					ord2Hash.put(longvalue, hashFunction.hash(longvalue));
					return longvalue;
				} catch (RuntimeException e) {
					throw Throwables.propagate(e);
				}
			}
		}

		@Override
		protected Long getDocGroupValue(int doc) {
			return fieldValue = docValues.get(doc);
		}
	}

	class TimeStampWeiwoDocValues
			extends
				WeiwoSegmentDocValues<NumericDocValues, Long, Long> {

		LightWeightLongMap<Long> ord2Hash;

		public TimeStampWeiwoDocValues(LeafReader leafReader,
				Field groupColumnName, boolean isCombineHash, Integer channel)
				throws IOException {
			super(DocValues.getNumeric(leafReader,
					groupColumnName.getColumnName()),
					new LongHashFunction(isCombineHash), groupColumnName,
					channel);
			ord2Hash = new LightWeightLongMap<>();
		}

		@Override
		public long hash(int doc) {
			long longvalue = getDocGroupValue(doc);
			Long hash = ord2Hash.get(longvalue);
			if (hash != null) {
				return hash;
			} else {
				try {
					ord2Hash.put(longvalue, hashFunction.hash(longvalue));
					return longvalue;
				} catch (RuntimeException e) {
					throw Throwables.propagate(e);
				}
			}
		}

		@Override
		protected Long getDocGroupValue(int doc) {
			return fieldValue = docValues.get(doc);
		}

		@Override
		public Object[] write(Object[] row) {
			row[channel] = fieldValue;
			return row;
		}
	}

	class DoubleWeiwoDocValues
			extends
				WeiwoSegmentDocValues<NumericDocValues, Long, Double> {

		LightWeightLongMap<Long> ord2Hash;

		public DoubleWeiwoDocValues(LeafReader leafReader,
				Field groupColumnName, boolean isCombineHash, Integer channel)
				throws IOException {
			super(DocValues.getNumeric(leafReader,
					groupColumnName.getColumnName()),
					new LongHashFunction(isCombineHash), groupColumnName,
					channel);
			ord2Hash = new LightWeightLongMap<>();
		}

		@Override
		protected Long getDocGroupValue(int doc) {
			return fieldValue = docValues.get(doc);
		}

		@Override
		public long hash(int doc) {
			long longvalue = getDocGroupValue(doc);
			Long hash = ord2Hash.get(longvalue);
			if (hash != null) {
				return hash;
			} else {
				try {
					ord2Hash.put(longvalue, hashFunction.hash(longvalue));
					return longvalue;
				} catch (RuntimeException e) {
					throw Throwables.propagate(e);
				}
			}
		}

		@Override
		public Object[] write(Object[] row) {
			row[channel] = Double.longBitsToDouble(fieldValue);
			return row;
		}
	}

	class FloatWeiwoDocValues
			extends
				WeiwoSegmentDocValues<NumericDocValues, Long, Float> {

		LightWeightLongMap<Long> ord2Hash;

		public FloatWeiwoDocValues(LeafReader leafReader, Field groupColumnName,
				boolean isCombineHash, Integer channel) throws IOException {
			super(DocValues.getNumeric(leafReader,
					groupColumnName.getColumnName()),
					new LongHashFunction(isCombineHash), groupColumnName,
					channel);
			ord2Hash = new LightWeightLongMap<>();
		}

		@Override
		public long hash(int doc) {
			Long longvalue = getDocGroupValue(doc);
			Long hash = ord2Hash.get(longvalue);
			if (hash != null) {
				return hash;
			} else {
				try {
					ord2Hash.put(longvalue, hashFunction.hash(longvalue));
					return longvalue;
				} catch (RuntimeException e) {
					throw Throwables.propagate(e);
				}
			}
		}

		@Override
		protected Long getDocGroupValue(int doc) {
			return fieldValue = docValues.get(doc);
		}

		@Override
		public Object[] write(Object[] row) {
			row[channel] = fieldValue;
			return row;
		}
	}

	class SortedWeiwoDocValues
			extends
				WeiwoSegmentDocValues<SortedDocValues, BytesRef, Slice> {

		LightWeightIntMap<Long> ord2Hash;

		BytesRef empty = new BytesRef();

		private boolean isDistinct;

		private long currentHashcode;

		public SortedWeiwoDocValues(LeafReader leafReader,
				Field groupColumnName, Integer channel) throws IOException {
			super(DocValues.getSorted(leafReader,
					groupColumnName.getColumnName()), new VarcharHashFunction(),
					groupColumnName, channel);
			ord2Hash = new LightWeightIntMap<>();
			// 分组字段，是否需要在查询结果中现实出来, 有些分组字段部需要显示比方说，select count(distinct
			// cookie_id) from weiwop8 where vpartition='1';
			// cookie_id 不需要再结果的传递中传递具体的value，则默认传递8字节的value，减少不必要的网络传输开销。
			isDistinct = groupColumnName.isDistinct();
		}

		@Override
		protected BytesRef getDocGroupValue(int orderID) {
			if (orderID == -1) {
				return fieldValue = empty;
			} else {
				long nanoGetDoc = System.nanoTime();
				fieldValue = docValues.lookupOrd(orderID);
				adapter.nanoGetLuceneDoc += System.nanoTime() - nanoGetDoc;
				return fieldValue;
			}
		}

		@Override
		public Object[] write(Object[] row) {
			Slice slice;
			if (isDistinct) {
				long nanoGetDoc = System.nanoTime();
				slice = Slices.wrappedLongArray(currentHashcode);
				adapter.nanoToSlice += System.nanoTime() - nanoGetDoc;
			} else {
				slice = Slices.utf8Slice(fieldValue.utf8ToString());
			}
			row[channel] = slice;
			return row;
		}

		@Override
		public long hash(int doc) {
			long nanoGetOrder = System.nanoTime();
			int newOrderId = docValues.getOrd(doc);
			adapter.nanoGetOrder += (System.nanoTime() - nanoGetOrder);
			long nanoOrd2HashGet = System.nanoTime();
			Long hash = ord2Hash.get(newOrderId);
			adapter.nanoOrder2HashGet += (System.nanoTime() - nanoOrd2HashGet);
			if (hash != null) {
				if (isDistinct) {
					currentHashcode = hash.longValue();
				} else {
					getDocGroupValue(newOrderId);
				}
				return hash;
			} else {
				try {
					currentHashcode = hashFunction
							.hash(getDocGroupValue(newOrderId));
					long nanoOrd2HashPut = System.nanoTime();
					ord2Hash.put(newOrderId, currentHashcode);
					adapter.nanoOrder2HashPuty += (System.nanoTime()
							- nanoOrd2HashPut);
					return currentHashcode;
				} catch (RuntimeException e) {
					throw Throwables.propagate(e);
				}
			}
		}
	}

	class TextWeiwoDocValues
			extends
				WeiwoSegmentDocValues<String, String, Slice> {

		private LeafReader leafReader;

		public TextWeiwoDocValues(String field, LeafReader leafReader,
				int channel) {
			super(field, new TextHashFunction(),
					new Field(field, isDistinct(field)), channel);
			this.leafReader = leafReader;
		}

		@Override
		public String getDocGroupValue(int doc) {
			try {
				return fieldValue = leafReader.document(doc).get(docValues);
			} catch (IOException e) {
				// log.error(e, "fail to get field from document");
				return "invalidField";
			}
		}

		@Override
		public Object[] write(Object[] row) {
			row[channel] = Slices.utf8Slice(fieldValue);
			return row;
		}
	}

	class CombineSegmentWriter implements SegmentWriter {

		private int hashChannel;

		private long combineHash;

		private List<WeiwoSegmentDocValues> docValues;

		public CombineSegmentWriter(List<WeiwoSegmentDocValues> docValues,
				int hashChannel) {
			this.docValues = requireNonNull(docValues, "docValue is null");
			this.hashChannel = hashChannel;
		}

		@Override
		public long hash(int docId) {
			long currentHashCode = 0L;
			for (WeiwoSegmentDocValues docValue : docValues) {
				currentHashCode = CombineHashFunction.getHash(currentHashCode,
						docValue.hash(docId));
			}
			return combineHash = currentHashCode;
		}

		@Override
		public Object[] write(Object[] object) {
			for (WeiwoSegmentDocValues docValue : docValues) {
				docValue.write(object);
			}
			object[hashChannel] = combineHash;
			return object;
		}

		@Override
		public boolean write(Object[] row, int docid) {
			write(row);
			return false;
		}
	}

	class SingleSegmentWriter implements SegmentWriter {

		private int hashChannel;

		private long hashValue;

		private WeiwoSegmentDocValues docValue;

		public SingleSegmentWriter(WeiwoSegmentDocValues docValue,
				Integer hashChannel) {
			this.docValue = requireNonNull(docValue, "docValue is null");
			this.hashChannel = hashChannel;
		}

		@Override
		public long hash(int doc) {
			return hashValue = docValue.hash(doc);
		}

		@Override
		public Object[] write(Object[] row) {
			docValue.write(row);
			row[hashChannel] = hashValue;
			return row;
		}

		@Override
		public boolean write(Object[] row, int docid) {
			hash(docid);
			write(row);
			return false;
		}
	}
}
