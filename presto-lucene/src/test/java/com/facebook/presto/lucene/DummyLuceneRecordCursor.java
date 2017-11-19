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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.FloatType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.TypeRegistry;

public class DummyLuceneRecordCursor extends LuceneRecordCursor {

	private int[] fieldToColumnIndex;
	private Map<Integer, LuceneColumnHandle> fieldColumnHandles;
	private Map<String, String> nameToLuceneType = new HashMap<>();
	static TypeRegistry typeManager;
	static {
		typeManager = new TypeRegistry();
		typeManager.addType(BigintType.BIGINT);
		typeManager.addType(IntegerType.INTEGER);
		typeManager.addType(VarcharType.VARCHAR);
		typeManager.addType(DoubleType.DOUBLE);
		typeManager.addType(FloatType.FLOAT);
		typeManager.addType(TimestampType.TIMESTAMP);
	}
	private List<LuceneColumnHandle> columnHandles;

	public DummyLuceneRecordCursor(List<LuceneColumnHandle> columnHandles) {
		super();
		this.columnHandles = columnHandles;
		this.fieldToColumnIndex = new int[columnHandles.size()];
		this.fieldColumnHandles = new HashMap<>();
		for (int i = 0; i < columnHandles.size(); i++) {
			LuceneColumnHandle columnHandle = columnHandles.get(i);
			this.fieldToColumnIndex[i] = columnHandle.getHiveColumnIndex();
			this.fieldColumnHandles.put(columnHandle.getHiveColumnIndex(),
					columnHandle);
			nameToLuceneType.put(columnHandle.getColumnName(),
					columnHandle.getLuceneType());
		}
	}

	@Override
	public String getLuceneType(String field) {
		return nameToLuceneType.get(field);
	}

	@Override
	public Type getType(int field) {
		int index = fieldToColumnIndex[field];
		LuceneColumnHandle column = fieldColumnHandles.get(index);
		return typeManager.getType(column.getTypeName());
	}

	@Override
	public String getColumnName(int field) {
		int index = fieldToColumnIndex[field];
		LuceneColumnHandle column = fieldColumnHandles.get(index);
		return column.getColumnName();
	}

	@Override
	public String getColumnLuceneType(int field) {
		int index = fieldToColumnIndex[field];
		LuceneColumnHandle column = fieldColumnHandles.get(index);
		return column.getLuceneType();
	}

	@Override
	public List<LuceneColumnHandle> getColumns() {
		return columnHandles;
	}
}