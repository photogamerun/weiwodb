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
package com.facebook.presto.lucene.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.facebook.presto.lucene.DummyLuceneRecordCursor;
import com.facebook.presto.lucene.LuceneAggColumnHandle;
import com.facebook.presto.lucene.LuceneColumnHandle;
import com.facebook.presto.lucene.LuceneHashColumnHandle;
import com.facebook.presto.lucene.WeiwoDBType;
import com.facebook.presto.lucene.hive.metastore.HiveType;
import com.facebook.presto.lucene.services.query.WeiwoCollector;
import com.facebook.presto.lucene.services.query.internals.WeiwoQueryService;
import com.facebook.presto.lucene.util.LuceneColumnUtil;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.LongVariableConstraint;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TypeVariableConstraint;
import com.facebook.presto.spi.pushdown.Pair;
import com.facebook.presto.spi.pushdown.PushDown;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.pair.AggregationPair;
import com.facebook.presto.sql.pair.GroupByPair;
import com.facebook.presto.sql.pair.OutputSignature;
import com.facebook.presto.sql.pair.SimpleFilterPair;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;

public class WeiwoQueryServiceTest {

	// public static void main(String[] args) throws IOException {
	// WeiwoQueryServiceTest te = new WeiwoQueryServiceTest();
	//
	// FSDirectory d = FSDirectory.open(FileSystems.getDefault().getPath(
	// "D:\\Documents\\weiwo\\press testing\\weiwo_20170207175216_9"));
	// te.reader = DirectoryReader.open(d);
	//
	// Expression expression = te
	// .getWhereExp("select count(*),sex from country group by sex");
	//
	// Symbol[] groupBySymbol = te
	// .getGroupBy("select count(*),sex from country group by sex");
	//
	// FunctionCall[] functions = te.getFunctionCall(
	// "select count(*),sex from country group by sex");
	//
	// PushDown pushdown = te.getPushDown(expression, functions,
	// groupBySymbol);
	//
	// List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();
	// columns.add(
	// new LuceneColumnHandle("testing", "sex", HiveType.HIVE_STRING,
	// 0, new TypeSignature("string"), WeiwoDBType.STRING));
	// columns.add(new LuceneAggColumnHandle("testing", "count",
	// HiveType.HIVE_LONG, 1, new TypeSignature("long"),
	// WeiwoDBType.LONG));
	//
	// DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);
	//
	// WeiwoQueryService handler = new WeiwoQueryService(te.reader, cursor);
	// // for (int i = 0; i < 100; i++) {
	// long starttime = System.currentTimeMillis();
	// WeiwoCollector collector = null;
	// try {
	// collector = handler.build(pushdown);
	// } catch (IOException e) {
	// e.printStackTrace();
	// Assert.fail();
	// }
	// Map<String, Long> map = new HashMap<String, Long>();
	// while (collector.hasNext()) {
	// map.put(collector.getSortedDocValues(0).toStringUtf8(),
	// collector.getLongDocValues(1));
	// }
	// System.out.println("map result " + map);
	// System.out.println(
	// "time elapse " + (System.currentTimeMillis() - starttime));
	// // }
	//
	// }

	DirectoryReader reader;

	@Before
	public void setUp() throws IOException {
		RAMDirectory directory = new RAMDirectory();
		IndexWriterConfig config = new IndexWriterConfig(new SimpleAnalyzer());
		IndexWriter writer = new IndexWriter(directory, config);
		List<City> citis = prepareData();
		for (City t : citis) {
			Document document = new Document();
			StringField provinceField = new StringField("province",
					t.getProvinceName(), Field.Store.YES);
			SortedDocValuesField _provinceField = new SortedDocValuesField(
					"province", new BytesRef(t.getProvinceName()));
			StringField cityField = new StringField("city", t.getCityName(),
					Field.Store.YES);
			TextField content = new TextField("content", "I love shanghai",
					Field.Store.YES);
			FloatPoint GDP = new FloatPoint("GDP", 123.12f);

			DoublePoint NaN = new DoublePoint("NaNDouble", Double.NaN);

			NumericDocValuesField _NaN = new NumericDocValuesField("NaNDouble",
					Double.doubleToLongBits(Double.NaN));

			NumericDocValuesField _timestamp = new NumericDocValuesField(
					"timestamp", 1447207200000L);
			LongPoint timestamp = new LongPoint("timestamp", 1447207200000L);

			int value = Float.floatToIntBits(123.12f);
			NumericDocValuesField _GDP = new NumericDocValuesField("GDP",
					value);

			SortedDocValuesField _cityField = new SortedDocValuesField("city",
					new BytesRef(t.getCityName()));
			SortedDocValuesField api = new SortedDocValuesField("api",
					new BytesRef(UUID.randomUUID().toString()));
			NumericDocValuesField num = new NumericDocValuesField("id", 3);
			LongPoint numLong = new LongPoint("id", 3);

			NumericDocValuesField num_int = new NumericDocValuesField("id_int",
					6);
			IntPoint numint = new IntPoint("id_int", 6);
			document.add(num_int);
			document.add(numint);
			document.add(NaN);
			document.add(_NaN);
			document.add(_timestamp);
			document.add(timestamp);
			document.add(GDP);
			document.add(_GDP);
			document.add(content);
			document.add(api);
			document.add(provinceField);
			document.add(_provinceField);
			document.add(cityField);
			document.add(_cityField);
			document.add(num);
			document.add(numLong);
			writer.addDocument(document);
		}
		// Document document = new Document();
		// NumericDocValuesField num = new NumericDocValuesField("id", 4);
		// LongPoint numLong = new LongPoint("id", 4);
		// document.add(numLong);
		// document.add(num);
		// writer.addDocument(document);
		writer.commit();
		writer.close();
		reader = DirectoryReader.open(directory);
	}

	private static List<City> prepareData() {

		City[] beijing = new City[]{new City("唯品会会员", "东城区"),
				new City("北京", "西城区"), new City("sd", "东城区"),
				new City("北京", "崇文区"), new City("北京", "宣武区"),
				new City("北京", "东城区"), new City("'北京'", "中关村")};
		City[] shanghai = new City[]{new City("上海", "黄浦区"),
				new City("难受香菇", "徐汇区"), new City("上海", "卢湾区"),
				new City("上海", "长宁区")};
		City[] tianjin = new City[]{new City("天津", "和平区"),
				new City("天津", "河东区"), new City("天津", "河西区")};
		List<City> cityList = new ArrayList<City>();
		cityList.addAll(Arrays.asList(beijing));
		cityList.addAll(Arrays.asList(shanghai));
		cityList.addAll(Arrays.asList(tianjin));
		return cityList;
	}

	@Test
	public void testNullValueQuery() throws IOException {
		Expression expression = getWhereExp(
				"select * from country where 1=1 and id=4");

		Symbol[] groupBySymbol = getGroupBy(
				"select * from country where 1=1 and id=4");

		PushDown pushdown = getPushDown(expression, null, groupBySymbol);

		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();
		columns.add(
				new LuceneColumnHandle("testing", "api", HiveType.HIVE_STRING,
						0, new TypeSignature("string"), WeiwoDBType.STRING));
		columns.add(
				new LuceneColumnHandle("testing", "city", HiveType.HIVE_STRING,
						1, new TypeSignature("string"), WeiwoDBType.STRING));

		columns.add(new LuceneColumnHandle("testing", "province",
				HiveType.HIVE_STRING, 2, new TypeSignature("string"),
				WeiwoDBType.STRING));

		columns.add(new LuceneColumnHandle("testing", "id", HiveType.HIVE_LONG,
				3, new TypeSignature("long"), WeiwoDBType.LONG));

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		long start = System.currentTimeMillis();
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		List<String> cities = new ArrayList<String>();
		while (collector.hasNext()) {
			cities.add(collector.getSortedDocValues(0).toStringUtf8());
		}
		System.out.println(cities);
		// Assert.assertEquals(4, cities.size());
		// Assert.assertTrue(cities.contains(123.12f));

		System.out.println("time cost " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testTimestampTypeQuery() throws IOException {
		Expression expression = getWhereExp(
				"select GDP from country where timestamp > cast('2015-11-10' as timestamp)");

		Symbol[] groupBySymbol = getGroupBy(
				"select GDP from country where timestamp > cast('2015-11-10' as timestamp)");

		PushDown pushdown = getPushDown(expression, null, groupBySymbol);

		LuceneColumnHandle column = new LuceneColumnHandle("testing", "GDP",
				HiveType.HIVE_FLOAT, 0, new TypeSignature("float"),
				WeiwoDBType.FLOAT);

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(
				Collections.singletonList(column));

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		long start = System.currentTimeMillis();
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		List<Float> cities = new ArrayList<Float>();
		while (collector.hasNext()) {
			cities.add(Float
					.intBitsToFloat(collector.getLongDocValues(0).intValue()));
		}
		Assert.assertEquals(14, cities.size());
		System.out.println(cities);
		Assert.assertTrue(cities.contains(123.12f));

		System.out.println("time cost " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testFloatTypeQuery() throws IOException {
		Expression expression = getWhereExp(
				"select GDP from country where 1=1 and province='北京'");

		Symbol[] groupBySymbol = getGroupBy(
				"select GDP from country where 1=1 and province='北京'");

		PushDown pushdown = getPushDown(expression, null, groupBySymbol);

		LuceneColumnHandle column = new LuceneColumnHandle("testing", "GDP",
				HiveType.HIVE_FLOAT, 0, new TypeSignature("float"),
				WeiwoDBType.FLOAT);

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(
				Collections.singletonList(column));

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		long start = System.currentTimeMillis();
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		List<Float> cities = new ArrayList<Float>();
		while (collector.hasNext()) {
			cities.add(Float
					.intBitsToFloat(collector.getLongDocValues(0).intValue()));
		}
		Assert.assertEquals(4, cities.size());
		Assert.assertTrue(cities.contains(123.12f));

		System.out.println("time cost " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testWhereOneEqualsOneQuery() throws IOException {
		Expression expression = getWhereExp(
				"select city from country where 1=1 and province='北京'");

		Symbol[] groupBySymbol = getGroupBy(
				"select city from country where 1=1 and province='北京'");

		PushDown pushdown = getPushDown(expression, null, groupBySymbol);

		LuceneColumnHandle column = new LuceneColumnHandle("testing", "city",
				HiveType.HIVE_STRING, 0, new TypeSignature("string"),
				WeiwoDBType.STRING);

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(
				Collections.singletonList(column));

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		long start = System.currentTimeMillis();
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		List<String> cities = new ArrayList<String>();
		while (collector.hasNext()) {
			cities.add(collector.getSortedDocValues(0).toStringUtf8());
		}

		Assert.assertTrue(cities.contains("东城区"));
		Assert.assertTrue(cities.contains("西城区"));
		Assert.assertTrue(cities.contains("崇文区"));
		Assert.assertTrue(cities.contains("宣武区"));

		System.out.println("time cost " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testIsNullInPredicatedQuery() throws IOException {
		Expression expression = getWhereExp(
				"select city from country where province in(null) ");

		Symbol[] groupBySymbol = getGroupBy(
				"select city from country where province in(null) ");

		PushDown pushdown = getPushDown(expression, null, groupBySymbol);

		LuceneColumnHandle column = new LuceneColumnHandle("testing", "city",
				HiveType.HIVE_STRING, 0, new TypeSignature("string"),
				WeiwoDBType.STRING);

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(
				Collections.singletonList(column));

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		long start = System.currentTimeMillis();
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		List<String> cities = new ArrayList<String>();
		while (collector.hasNext()) {
			cities.add(collector.getSortedDocValues(0).toStringUtf8());
		}

		Assert.assertTrue(cities.isEmpty());

		System.out.println("time cost " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testIsNullPredicatedQuery() throws IOException {
		Expression expression = getWhereExp(
				"select city from country where province is null ");

		Symbol[] groupBySymbol = getGroupBy(
				"select city from country where province is null ");

		PushDown pushdown = getPushDown(expression, null, groupBySymbol);

		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();

		columns.add(
				new LuceneColumnHandle("testing", "city", HiveType.HIVE_STRING,
						0, new TypeSignature("string"), WeiwoDBType.STRING));

		columns.add(new LuceneColumnHandle("testing", "province",
				HiveType.HIVE_STRING, 1, new TypeSignature("string"),
				WeiwoDBType.STRING));

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		long start = System.currentTimeMillis();
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		List<String> cities = new ArrayList<String>();
		while (collector.hasNext()) {
			cities.add(collector.getSortedDocValues(0).toStringUtf8());
		}

		Assert.assertTrue(cities.isEmpty());

		System.out.println("time cost " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testIsNotNullPredicatedQuery() throws IOException {
		Expression expression = getWhereExp(
				"select city from country where province is not null ");

		Symbol[] groupBySymbol = getGroupBy(
				"select city from country where province is not null ");

		PushDown pushdown = getPushDown(expression, null, groupBySymbol);

		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();
		columns.add(
				new LuceneColumnHandle("testing", "city", HiveType.HIVE_STRING,
						0, new TypeSignature("string"), WeiwoDBType.STRING));
		columns.add(new LuceneColumnHandle("testing", "province",
				HiveType.HIVE_STRING, 1, new TypeSignature("string"),
				WeiwoDBType.STRING));

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		long start = System.currentTimeMillis();
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		List<String> cities = new ArrayList<String>();
		while (collector.hasNext()) {
			cities.add(collector.getSortedDocValues(0).toStringUtf8());
		}

		Assert.assertTrue(cities.contains("东城区"));
		Assert.assertTrue(cities.contains("东城区"));
		Assert.assertTrue(cities.contains("中关村"));
		Assert.assertTrue(cities.contains("黄浦区"));
		Assert.assertTrue(cities.contains("徐汇区"));
		Assert.assertTrue(cities.contains("卢湾区"));
		Assert.assertTrue(cities.contains("长宁区"));
		Assert.assertTrue(cities.contains("和平区"));
		Assert.assertTrue(cities.contains("河东区"));
		Assert.assertTrue(cities.contains("河西区"));

		System.out.println("time cost " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testIsBeijinNotInPredicatedQuery() throws IOException {
		Expression expression = getWhereExp(
				"select city from country where province not in(('北京')) ");

		Symbol[] groupBySymbol = getGroupBy(
				"select city from country where province not in(('北京')) ");

		PushDown pushdown = getPushDown(expression, null, groupBySymbol);

		LuceneColumnHandle column = new LuceneColumnHandle("testing", "city",
				HiveType.HIVE_STRING, 0, new TypeSignature("string"),
				WeiwoDBType.STRING);

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(
				Collections.singletonList(column));

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		long start = System.currentTimeMillis();
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		List<String> cities = new ArrayList<String>();
		while (collector.hasNext()) {
			cities.add(collector.getSortedDocValues(0).toStringUtf8());
		}
		System.out.println(cities);

		Assert.assertTrue(cities.contains("东城区"));
		Assert.assertTrue(cities.contains("东城区"));
		Assert.assertTrue(cities.contains("中关村"));
		Assert.assertTrue(cities.contains("黄浦区"));
		Assert.assertTrue(cities.contains("徐汇区"));
		Assert.assertTrue(cities.contains("卢湾区"));
		Assert.assertTrue(cities.contains("长宁区"));
		Assert.assertTrue(cities.contains("和平区"));
		Assert.assertTrue(cities.contains("河东区"));
		Assert.assertTrue(cities.contains("河西区"));
		System.out.println("time cost " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testIsNullNotInPredicatedQuery() throws IOException {
		Expression expression = getWhereExp(
				"select city from country where province not in(null,null) ");

		Symbol[] groupBySymbol = getGroupBy(
				"select city from country where province not in(null,null) ");

		PushDown pushdown = getPushDown(expression, null, groupBySymbol);

		LuceneColumnHandle column = new LuceneColumnHandle("testing", "city",
				HiveType.HIVE_STRING, 0, new TypeSignature("string"),
				WeiwoDBType.STRING);

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(
				Collections.singletonList(column));

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		long start = System.currentTimeMillis();
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		List<String> cities = new ArrayList<String>();
		while (collector.hasNext()) {
			cities.add(collector.getSortedDocValues(0).toStringUtf8());
		}

		Assert.assertEquals(14, cities.size());

		System.out.println("time cost " + (System.currentTimeMillis() - start));
	}

	// @Test
	// public void testIsNotNullQuery() throws IOException {
	// Expression expression = getWhereExp(
	// "select city from country where province <> null ");
	//
	// Symbol[] groupBySymbol = getGroupBy(
	// "select city from country where province <> null ");
	//
	// PushDown pushdown = getPushDown(expression, null, groupBySymbol);
	//
	// LuceneColumnHandle column = new LuceneColumnHandle("testing", "city",
	// HiveType.HIVE_STRING, 0, new TypeSignature("string"),
	// WeiwoDBType.STRING);
	//
	// DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(
	// Collections.singletonList(column));
	//
	// WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
	// WeiwoCollector collector = null;
	// long start = System.currentTimeMillis();
	// try {
	// collector = handler.build(pushdown);
	// } catch (IOException e) {
	// e.printStackTrace();
	// Assert.fail();
	// }
	// List<String> cities = new ArrayList<String>();
	// while (collector.hasNext()) {
	// cities.add(collector.getSortedDocValues(0).toStringUtf8());
	// }
	//
	// Assert.assertTrue(cities.contains("东城区"));
	// Assert.assertTrue(cities.contains("西城区"));
	// Assert.assertTrue(cities.contains("崇文区"));
	// Assert.assertTrue(cities.contains("宣武区"));
	// Assert.assertTrue(cities.contains("东城区"));
	// Assert.assertTrue(cities.contains("黄浦区"));
	// Assert.assertTrue(cities.contains("卢湾区"));
	// Assert.assertTrue(cities.contains("徐汇区"));
	// Assert.assertTrue(cities.contains("长宁区"));
	// Assert.assertTrue(cities.contains("和平区"));
	// Assert.assertTrue(cities.contains("河东区"));
	// Assert.assertTrue(cities.contains("河西区"));
	//
	// System.out.println("time cost " + (System.currentTimeMillis() - start));
	//
	// expression = getWhereExp(
	// "select city from country where province != (null) ");
	//
	// groupBySymbol = getGroupBy(
	// "select city from country where province != (null) ");
	//
	// pushdown = getPushDown(expression, null, groupBySymbol);
	//
	// column = new LuceneColumnHandle("testing", "city", HiveType.HIVE_STRING,
	// 0, new TypeSignature("string"), WeiwoDBType.STRING);
	//
	// cursor = new DummyLuceneRecordCursor(Collections.singletonList(column));
	//
	// handler = new WeiwoQueryService(reader, cursor);
	// collector = null;
	// start = System.currentTimeMillis();
	// try {
	// collector = handler.build(pushdown);
	// } catch (IOException e) {
	// e.printStackTrace();
	// Assert.fail();
	// }
	// cities = new ArrayList<String>();
	// while (collector.hasNext()) {
	// cities.add(collector.getSortedDocValues(0).toStringUtf8());
	// }
	//
	// Assert.assertTrue(cities.contains("东城区"));
	// Assert.assertTrue(cities.contains("西城区"));
	// Assert.assertTrue(cities.contains("崇文区"));
	// Assert.assertTrue(cities.contains("宣武区"));
	// Assert.assertTrue(cities.contains("东城区"));
	// Assert.assertTrue(cities.contains("黄浦区"));
	// Assert.assertTrue(cities.contains("卢湾区"));
	// Assert.assertTrue(cities.contains("徐汇区"));
	// Assert.assertTrue(cities.contains("长宁区"));
	// Assert.assertTrue(cities.contains("和平区"));
	// Assert.assertTrue(cities.contains("河东区"));
	// Assert.assertTrue(cities.contains("河西区"));
	//
	// System.out.println("time cost " + (System.currentTimeMillis() - start));
	//
	// }

	@Test
	public void testTextFieldAsGroupByItemQuery() throws IOException {
		System.out.println("start");
		Expression expression = getWhereExp(
				"select count(*) from country group by content");

		Symbol[] groupBySymbol = getGroupBy(
				"select count(*) from country group by content");

		FunctionCall[] functions = getFunctionCall(
				"select count(*) from country group by content");

		PushDown pushdown = getPushDown(expression, functions, groupBySymbol);

		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();
		columns.add(new LuceneColumnHandle("testing", "content",
				HiveType.HIVE_STRING, 0, new TypeSignature("text"),
				WeiwoDBType.TEXT));

		columns.add(new LuceneAggColumnHandle("testing", "count",
				HiveType.HIVE_LONG, 1, new TypeSignature("long"),
				WeiwoDBType.LONG));

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		// for (int i = 0; i < 100; i++) {
		long start = System.currentTimeMillis();
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}

		while (collector.hasNext()) {
			System.out.println(collector.getSortedDocValues(0).toStringUtf8()
					+ " " + collector.getLongDocValues(1));
		}
		System.out.println(System.currentTimeMillis() - start);
	}

	@Test
	public void testSameGroupByFieldShareWithSameHashCodeWithMutipleDotQuery()
			throws IOException {
		System.out.println("start");
		Expression expression = getWhereExp(
				"select count(*) from country where province ='''北京''' group by province");

		Symbol[] groupBySymbol = getGroupBy(
				"select count(*) from country where province ='''北京''' group by province");

		FunctionCall[] functions = getFunctionCall(
				"select count(*) from country where province ='''北京''' group by province");

		PushDown pushdown = getPushDown(expression, functions, groupBySymbol);

		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();
		columns.add(new LuceneColumnHandle("testing", "province",
				HiveType.HIVE_STRING, 0, new TypeSignature("String"),
				WeiwoDBType.STRING));

		columns.add(new LuceneHashColumnHandle("testing",
				LuceneColumnUtil.HASHCODE, HiveType.HIVE_LONG, 1,
				new TypeSignature("long"), WeiwoDBType.LONG));

		columns.add(new LuceneAggColumnHandle("testing", "count",
				HiveType.HIVE_LONG, 2, new TypeSignature("long"),
				WeiwoDBType.LONG));

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		// for (int i = 0; i < 100; i++) {
		long start = System.currentTimeMillis();
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}

		Set<Long> hashcodeNumber = new HashSet<Long>();

		while (collector.hasNext()) {
			hashcodeNumber.add(collector.getLongDocValues(1));
		}

		Assert.assertEquals(1, hashcodeNumber.size());
		System.out.println(System.currentTimeMillis() - start);
	}

	@Test
	public void testSameGroupByFieldShareWithSameHashCodeQuery()
			throws IOException {
		System.out.println("start");
		Expression expression = getWhereExp(
				"select count(*) from country where province ='北京' group by province");

		Symbol[] groupBySymbol = getGroupBy(
				"select count(*) from country where province ='北京' group by province");

		FunctionCall[] functions = getFunctionCall(
				"select count(*) from country where province ='北京' group by province");

		PushDown pushdown = getPushDown(expression, functions, groupBySymbol);

		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();
		columns.add(new LuceneColumnHandle("testing", "province",
				HiveType.HIVE_STRING, 0, new TypeSignature("String"),
				WeiwoDBType.STRING));

		columns.add(new LuceneHashColumnHandle("testing",
				LuceneColumnUtil.HASHCODE, HiveType.HIVE_LONG, 1,
				new TypeSignature("long"), WeiwoDBType.LONG));

		columns.add(new LuceneAggColumnHandle("testing", "count",
				HiveType.HIVE_LONG, 2, new TypeSignature("long"),
				WeiwoDBType.LONG));

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		for (int i = 0; i < 100; i++) {
			long start = System.currentTimeMillis();
			try {
				collector = handler.build(pushdown);
			} catch (IOException e) {
				e.printStackTrace();
				Assert.fail();
			}

			Set<Long> hashcodeNumber = new HashSet<Long>();

			while (collector.hasNext()) {
				System.out.println(collector.getLongDocValues(2));
			}

			// Assert.assertEquals(1, hashcodeNumber.size());
			System.out.println(System.currentTimeMillis() - start);
		}
	}

	@Test
	public void testTextFieldQuery() throws IOException {
		Expression expression = getWhereExp(
				"select content from country where 1=1");

		Symbol[] groupBySymbol = getGroupBy(
				"select content from country where 1=1");

		FunctionCall[] functions = getFunctionCall(
				"select content from country where 1=1");

		PushDown pushdown = getPushDown(expression, functions, groupBySymbol);

		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();
		columns.add(new LuceneColumnHandle("testing", "content",
				HiveType.HIVE_STRING, 0, new TypeSignature("text"),
				WeiwoDBType.TEXT));

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		// for (int i = 0; i < 100; i++) {
		long start = System.currentTimeMillis();
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}

		List<String> cities = new ArrayList<String>();

		while (collector.hasNext()) {
			cities.add(collector.getSortedDocValues(0).toStringUtf8());
		}

		Assert.assertEquals(14, cities.size());

		Assert.assertEquals("I love shanghai", cities.get(0));

		System.out.println(System.currentTimeMillis() - start);
	}

	@Test
	public void testLikeQuery() throws IOException {
		Expression expression = getWhereExp(
				"select city from country where province like '%北%%京%%%%%' ");

		Symbol[] groupBySymbol = getGroupBy(
				"select city from country where province like '%北%%京%%%%%' ");

		// FunctionCall[] functions = getFunctionCall(
		// "select city from country where province like '%北%%京%%%%%'");

		PushDown pushdown = getPushDown(expression, null, groupBySymbol);

		LuceneColumnHandle column = new LuceneColumnHandle("testing", "city",
				HiveType.HIVE_STRING, 0, new TypeSignature("string"),
				WeiwoDBType.STRING);

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(
				Collections.singletonList(column));

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		long start = System.currentTimeMillis();
		try {
			handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}

		System.out.println("time cost " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testNaNquery() throws IOException {// TODO
		Expression expression = getWhereExp(
				"select city from country where NaNDouble = NaN ");

		Symbol[] groupBySymbol = getGroupBy(
				"select city from country where NaNDouble = NaN ");

		// FunctionCall[] functions = getFunctionCall(
		// "select city from country where province like '%北%%京%%%%%'");

		PushDown pushdown = getPushDown(expression, null, groupBySymbol);

		LuceneColumnHandle column = new LuceneColumnHandle("testing", "city",
				HiveType.HIVE_STRING, 0, new TypeSignature("string"),
				WeiwoDBType.STRING);

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(
				Collections.singletonList(column));

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		long start = System.currentTimeMillis();
		WeiwoCollector collector = null;
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		while (collector.hasNext()) {
			System.out.println(collector.getSortedDocValues(0).toStringUtf8());
		}

		System.out.println("time cost " + (System.currentTimeMillis() - start));

	}

	@Test
	public void testCountDistinctQuery() throws IOException {
		Expression expression = getWhereExp(
				"select provice,city from country group by province,city");

		Symbol[] groupBySymbol = getGroupBy(
				"select provice,city from country group by province,city");

		FunctionCall[] functions = getFunctionCall(
				"select provice,city from country group by province,city");

		PushDown pushdown = getPushDown(expression, functions, groupBySymbol);

		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();

		columns.add(new LuceneColumnHandle("testing", "province",
				HiveType.HIVE_STRING, 0, new TypeSignature("string"),
				WeiwoDBType.STRING));

		columns.add(
				new LuceneColumnHandle("testing", "city", HiveType.HIVE_STRING,
						1, new TypeSignature("text"), WeiwoDBType.STRING));

		columns.add(new LuceneHashColumnHandle("testing",
				LuceneColumnUtil.HASHCODE, HiveType.HIVE_LONG, 2,
				new TypeSignature("long"), WeiwoDBType.LONG));

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		for (int i = 0; i < 100; i++) {
			long start = System.currentTimeMillis();
			try {
				collector = handler.build(pushdown);
			} catch (IOException e) {
				e.printStackTrace();
				Assert.fail();
			}
			int size = 0;
			while (collector.hasNext()) {
				String a = collector.getSortedDocValues(0).toStringUtf8() + " "
						+ collector.getSortedDocValues(1).toStringUtf8() + " "
						+ collector.getLongDocValues(2);
				size++;
			}
			System.out.println("size " + size);

			System.out.println(System.currentTimeMillis() - start);
		}
	}

	// public static void main(String[] args) throws ParseException {
	//
	// SimpleDateFormat dataformat = new SimpleDateFormat(
	// "yyyy-MM-dd HH:mm:ss");
	// dataformat.parse("2015-11-11 10:00:00");
	// }

	// @Test
	// public void testCastQuery() throws IOException {
	// Expression expression = getWhereExp(
	// "select count(*) from country where province > cast('2015-11-11 10:00:00'
	// as timestamp)");
	//
	// FunctionCall[] functions = getFunctionCall(
	// "select count(*) from country where province > cast('2015-11-11 10:00:00'
	// as timestamp)");
	//
	// PushDown pushdown = getPushDown(expression, functions, null);
	// WeiwoQueryService handler = new WeiwoQueryService(reader);
	// WeiwoCollector collector = null;
	// // for (int i = 0; i < 100; i++) {
	// long start = System.currentTimeMillis();
	// try {
	// collector = handler.build(pushdown);
	// } catch (IOException e) {
	// e.printStackTrace();
	// Assert.fail();
	// }
	// // System.out.println(
	// // "time cost " + (System.currentTimeMillis() - start));
	// while (collector.hasNext()) {
	// for (FunctionCall functionCall : functions) {
	// System.out.println(collector
	// .getLongDocValues(functionCall.getName().getSuffix()));
	// }
	// }
	// // }
	// }

	@Test
	public void testDoubleGroupQuery() throws IOException {
		Expression expression = getWhereExp(
				"select count(*) from country where id=3");

		Symbol[] groupBySymbol = getGroupBy(
				"select count(*) from country where id=3");

		FunctionCall[] functions = getFunctionCall(
				"select count(*) from country where id=3");

		PushDown pushdown = getPushDown(expression, functions, groupBySymbol);

		LuceneAggColumnHandle column = new LuceneAggColumnHandle("testing",
				"count", HiveType.HIVE_LONG, 0, new TypeSignature("long"),
				WeiwoDBType.LONG);

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(
				Collections.singletonList(column));

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		// for (int i = 0; i < 100; i++) {
		long start = System.currentTimeMillis();
		try {
			handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		// while (collector.hasNext()) {
		// System.out.println(collector.getLongDocValues(0));
		// }
		long time = System.currentTimeMillis();
		System.out.println("time cost " + (time - start));
		// }
	}

	@Test
	public void testDistinctGroupQuery() throws IOException {
		Expression expression = getWhereExp(
				"select province,city from country where id=3 group by province,city");

		Symbol[] groupBySymbol = getGroupBy(
				"select province,city from country where id=3 group by province,city");

		FunctionCall[] functions = getFunctionCall(
				"select province,city from country where id=3 group by province,city");

		PushDown pushdown = getPushDown(expression, functions, groupBySymbol);

		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();

		LuceneColumnHandle column1 = new LuceneColumnHandle("testing",
				"province", HiveType.HIVE_STRING, 0,
				new TypeSignature("string"), WeiwoDBType.STRING);

		LuceneColumnHandle column2 = new LuceneColumnHandle("testing", "city",
				HiveType.HIVE_STRING, 1, new TypeSignature("string"),
				WeiwoDBType.STRING);

		LuceneColumnHandle column3 = new LuceneHashColumnHandle("testing",
				"hashValue", HiveType.HIVE_LONG, 2, new TypeSignature("long"),
				WeiwoDBType.LONG);

		columns.add(column1);
		columns.add(column2);
		columns.add(column3);

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		// for (int i = 0; i < 100; i++) {
		long start = System.currentTimeMillis();
		WeiwoCollector collector = null;
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}

		List<CityInProvince> cities = new ArrayList<CityInProvince>();

		while (collector.hasNext()) {
			System.out.println(collector.getSortedDocValues(0).toStringUtf8()
					+ " : " + collector.getSortedDocValues(1).toStringUtf8());
			cities.add(new CityInProvince(
					collector.getSortedDocValues(0).toStringUtf8(),
					collector.getSortedDocValues(1).toStringUtf8()));
		}
		System.out.println(cities);
		long time = System.currentTimeMillis();
		System.out.println("time cost " + (time - start));
		// }
	}

	private class CityInProvince {

		private String province;

		private String city;

		private CityInProvince(String province, String city) {
			this.province = province;
			this.city = city;
		}

		@Override
		public String toString() {
			return province + " : " + city;
		}
	}

	// Assert.assertEquals(1, collector.size());

	@Test
	public void testCountDistinct_GroupBy() throws IOException {
		System.out.println("start");
		Expression expression = getWhereExp(
				"select count(*) from country group by api");

		Symbol[] groupBySymbol = getGroupBy(
				"select count(*) from country group by api");

		FunctionCall[] functions = getFunctionCall(
				"select count(*) from country group by api");

		PushDown pushdown = getPushDown(expression, functions, groupBySymbol);

		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();
		columns.add(
				new LuceneColumnHandle("testing", "api", HiveType.HIVE_STRING,
						0, new TypeSignature("string"), WeiwoDBType.STRING));
		columns.add(new LuceneAggColumnHandle("testing", "count",
				HiveType.HIVE_LONG, 1, new TypeSignature("long"),
				WeiwoDBType.LONG));

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		// for (int i = 0; i < 100; i++) {
		long start = System.currentTimeMillis();
		try {
			handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}

		// while (collector.hasNext()) {
		// System.out.println(collector.getSortedDocValues(0) + " "
		// + collector.getLongDocValues(1));
		// }
		System.out.println(System.currentTimeMillis() - start);
		// }

	}

	@Test
	public void testWildCardQuery() throws IOException {
		Expression expression = getWhereExp(
				"select * from country where province like '%北%%京%%%%%'");
		Symbol[] groupBySymbol = getGroupBy(
				"select * from country where province like '%北%%京%%%%%'");
		PushDown pushdown = getPushDown(expression, null, groupBySymbol);
		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();
		columns.add(
				new LuceneColumnHandle("testing", "api", HiveType.HIVE_STRING,
						0, new TypeSignature("string"), WeiwoDBType.STRING));
		columns.add(
				new LuceneColumnHandle("testing", "city", HiveType.HIVE_STRING,
						1, new TypeSignature("string"), WeiwoDBType.STRING));

		columns.add(new LuceneColumnHandle("testing", "province",
				HiveType.HIVE_STRING, 2, new TypeSignature("string"),
				WeiwoDBType.STRING));

		columns.add(new LuceneColumnHandle("testing", "id", HiveType.HIVE_LONG,
				3, new TypeSignature("long"), WeiwoDBType.LONG));
		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);
		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		// for (int i = 0; i < 100; i++) {
		long starttime = System.currentTimeMillis();
		WeiwoCollector collector = null;
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		System.out.println(
				"time elapse " + (System.currentTimeMillis() - starttime));
		System.out.println("size of data " + collector.size());

		Assert.assertEquals(5, collector.size());

		List<String> citis = new ArrayList<String>();

		while (collector.hasNext()) {
			citis.add(collector.getSortedDocValues(1).toStringUtf8());
		}
		// }
		System.out.println(citis);
		Assert.assertTrue(citis.contains("东城区"));
		Assert.assertTrue(citis.contains("西城区"));
		Assert.assertTrue(citis.contains("崇文区"));
		Assert.assertTrue(citis.contains("宣武区"));

	}

	@Test
	public void testQueryBetween_on_integerColumn() throws IOException {
		Expression expression = getWhereExp(
				"select * from country where id_int between 5 and 6");
		Symbol[] groupBySymbol = getGroupBy(
				"select * from country where id_int between 5 and 6");
		PushDown pushdown = getPushDown(expression, null, groupBySymbol);
		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();
		columns.add(
				new LuceneColumnHandle("testing", "api", HiveType.HIVE_STRING,
						0, new TypeSignature("string"), WeiwoDBType.STRING));
		columns.add(
				new LuceneColumnHandle("testing", "city", HiveType.HIVE_STRING,
						1, new TypeSignature("string"), WeiwoDBType.STRING));

		columns.add(new LuceneColumnHandle("testing", "province",
				HiveType.HIVE_STRING, 2, new TypeSignature("string"),
				WeiwoDBType.STRING));

		columns.add(
				new LuceneColumnHandle("testing", "id_int", HiveType.HIVE_INT,
						3, new TypeSignature("int"), WeiwoDBType.INT));
		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);
		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		List<String> cities = new ArrayList<String>();

		while (collector.hasNext()) {
			cities.add(collector.getSortedDocValues(1).toStringUtf8());
		}

		Assert.assertTrue(cities.contains("东城区"));
		Assert.assertTrue(cities.contains("西城区"));
		Assert.assertTrue(cities.contains("崇文区"));
		Assert.assertTrue(cities.contains("宣武区"));
		Assert.assertTrue(cities.contains("东城区"));
		Assert.assertTrue(cities.contains("黄浦区"));
		Assert.assertTrue(cities.contains("卢湾区"));
		Assert.assertTrue(cities.contains("徐汇区"));
		Assert.assertTrue(cities.contains("长宁区"));
		Assert.assertTrue(cities.contains("和平区"));
		Assert.assertTrue(cities.contains("河东区"));
		Assert.assertTrue(cities.contains("河西区"));

		Assert.assertEquals(14, cities.size());
	}

	@Test
	public void testQueryBetween_on_longColumn() throws IOException {
		Expression expression = getWhereExp(
				"select * from country where id between 1 and 4");
		Symbol[] groupBySymbol = getGroupBy(
				"select * from country where id between 1 and 4");
		PushDown pushdown = getPushDown(expression, null, groupBySymbol);
		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();
		columns.add(
				new LuceneColumnHandle("testing", "api", HiveType.HIVE_STRING,
						0, new TypeSignature("string"), WeiwoDBType.STRING));
		columns.add(
				new LuceneColumnHandle("testing", "city", HiveType.HIVE_STRING,
						1, new TypeSignature("string"), WeiwoDBType.STRING));

		columns.add(new LuceneColumnHandle("testing", "province",
				HiveType.HIVE_STRING, 2, new TypeSignature("string"),
				WeiwoDBType.STRING));

		columns.add(new LuceneColumnHandle("testing", "id", HiveType.HIVE_LONG,
				3, new TypeSignature("long"), WeiwoDBType.LONG));
		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);
		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		WeiwoCollector collector = null;
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		List<String> cities = new ArrayList<String>();

		while (collector.hasNext()) {
			cities.add(collector.getSortedDocValues(1).toStringUtf8());
		}

		Assert.assertTrue(cities.contains("东城区"));
		Assert.assertTrue(cities.contains("西城区"));
		Assert.assertTrue(cities.contains("崇文区"));
		Assert.assertTrue(cities.contains("宣武区"));
		Assert.assertTrue(cities.contains("东城区"));
		Assert.assertTrue(cities.contains("黄浦区"));
		Assert.assertTrue(cities.contains("卢湾区"));
		Assert.assertTrue(cities.contains("徐汇区"));
		Assert.assertTrue(cities.contains("长宁区"));
		Assert.assertTrue(cities.contains("和平区"));
		Assert.assertTrue(cities.contains("河东区"));
		Assert.assertTrue(cities.contains("河西区"));
		Assert.assertTrue(cities.contains("中关村"));
		Assert.assertTrue(cities.contains("东城区"));

		Assert.assertEquals(14, cities.size());
	}

	@Test
	public void testQueryPushDown_like() throws IOException {
		Expression expression = getWhereExp(
				"select * from country where province like '北%'");
		Symbol[] groupBySymbol = getGroupBy(
				"select * from country where province like '北%'");

		PushDown pushdown = getPushDown(expression, null, groupBySymbol);
		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();
		columns.add(
				new LuceneColumnHandle("testing", "api", HiveType.HIVE_STRING,
						0, new TypeSignature("string"), WeiwoDBType.STRING));
		columns.add(
				new LuceneColumnHandle("testing", "city", HiveType.HIVE_STRING,
						1, new TypeSignature("string"), WeiwoDBType.STRING));

		columns.add(new LuceneColumnHandle("testing", "province",
				HiveType.HIVE_STRING, 2, new TypeSignature("string"),
				WeiwoDBType.STRING));

		columns.add(new LuceneColumnHandle("testing", "id", HiveType.HIVE_LONG,
				3, new TypeSignature("long"), WeiwoDBType.LONG));
		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);
		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		long starttime = System.currentTimeMillis();
		WeiwoCollector collector = null;
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}

		Assert.assertEquals(4, collector.size());

		List<String> citis = new ArrayList<String>();

		while (collector.hasNext()) {
			citis.add(collector.getSortedDocValues(1).toStringUtf8());
		}
		// System.out.println(citis);
		Assert.assertTrue(citis.contains("东城区"));
		Assert.assertTrue(citis.contains("西城区"));
		Assert.assertTrue(citis.contains("崇文区"));
		Assert.assertTrue(citis.contains("宣武区"));
		System.out.println(
				"time elapse " + (System.currentTimeMillis() - starttime));
	}

	// 不支持单独分组没有聚合的分组查询 没有意义。
	// @Test
	// public void testBuildPushDownFilter_groupby() throws IOException {
	// Expression expression = getWhereExp(
	// "select * from country where id >1 group by city");
	//
	// Symbol[] groupBySymbol = getGroupBy(
	// "select * from country where id >1 group by city");
	//
	// PushDown pushdown = getPushDown(expression, null, groupBySymbol);
	//
	// WeiwoQueryService handler = new WeiwoQueryService(reader);
	// long starttime = System.currentTimeMillis();
	// WeiwoCollector collector = null;
	// try {
	// collector = handler.build(pushdown);
	// } catch (IOException e) {
	// e.printStackTrace();
	// Assert.fail();
	// }
	//
	// Assert.assertEquals(11, collector.size());
	// while (collector.hasNext()) {
	// System.out.println(collector.getSortedDocValues("city")
	// .utf8ToString() + " "
	// + collector.getSortedDocValues("province").utf8ToString());
	// }
	// System.out.println(
	// "time elapse " + (System.currentTimeMillis() - starttime));
	// }

	@Test
	public void testBuildPushDownFilter_groupby_count() throws IOException {
		Expression expression = getWhereExp(
				"select count(*),province from country where id >1 group by province");

		Symbol[] groupBySymbol = getGroupBy(
				"select count(*),province from country where id >1 group by province");

		FunctionCall[] functions = getFunctionCall(
				"select count(*),province from country where id >1 group by province");

		PushDown pushdown = getPushDown(expression, functions, groupBySymbol);

		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();
		columns.add(new LuceneColumnHandle("testing", "province",
				HiveType.HIVE_STRING, 0, new TypeSignature("string"),
				WeiwoDBType.STRING));
		columns.add(new LuceneAggColumnHandle("testing", "count",
				HiveType.HIVE_LONG, 1, new TypeSignature("long"),
				WeiwoDBType.LONG));

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		// for (int i = 0; i < 100; i++) {
		long starttime = System.currentTimeMillis();
		WeiwoCollector collector = null;
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		Map<String, Long> map = new HashMap<String, Long>();
		while (collector.hasNext()) {
			map.put(collector.getSortedDocValues(0).toStringUtf8(),
					collector.getLongDocValues(1));
		}
		System.out.println("map result " + map);
		System.out.println(
				"time elapse " + (System.currentTimeMillis() - starttime));
		// }
		// while (collector.hasNext()) {
		// System.out.println(collector.getSortedDocValues("province")
		// + " " + collector.getLongDocValues("count"));
		//
		// }
	}

	@Test
	public void testBuildPushDownFilter_count() throws IOException {
		Expression expression = getWhereExp(
				"select count(*) from country where id >1");

		Symbol[] groupBySymbol = getGroupBy(
				"select count(*) from country where id >1");

		FunctionCall[] functions = getFunctionCall(
				"select count(*) from country where id >1");

		PushDown pushdown = getPushDown(expression, functions, groupBySymbol);

		LuceneAggColumnHandle column = new LuceneAggColumnHandle("testing",
				"count", HiveType.HIVE_LONG, 0, new TypeSignature("long"),
				WeiwoDBType.LONG);

		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(
				Collections.singletonList(column));

		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		// for (int i = 0; i < 100; i++) {
		long starttime = System.currentTimeMillis();
		WeiwoCollector collector = null;
		try {
			collector = handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}

		Assert.assertEquals(1, collector.size());

		// while (collector.hasNext()) {
		// System.out.println(collector.getLongDocValues("count"));
		//
		// }
		System.out.println(
				"time elapse " + (System.currentTimeMillis() - starttime));
		// }
		// while (collector.hasNext()) {
		// System.out.println(collector.getNumericDocValues(functions[0]));
		//
		// }

	}

	@Test
	public void testBuildPushDownFilter() throws IOException {

		Expression expression = getWhereExp(
				"select * from country where id >1");

		Symbol[] groupBySymbol = getGroupBy(
				"select * from country where id >1");

		PushDown pushdown = getPushDown(expression, null, groupBySymbol);

		List<LuceneColumnHandle> columns = new ArrayList<LuceneColumnHandle>();
		columns.add(
				new LuceneColumnHandle("testing", "api", HiveType.HIVE_STRING,
						0, new TypeSignature("string"), WeiwoDBType.STRING));
		columns.add(
				new LuceneColumnHandle("testing", "city", HiveType.HIVE_STRING,
						1, new TypeSignature("string"), WeiwoDBType.STRING));

		columns.add(new LuceneColumnHandle("testing", "province",
				HiveType.HIVE_STRING, 2, new TypeSignature("string"),
				WeiwoDBType.STRING));

		columns.add(new LuceneColumnHandle("testing", "id", HiveType.HIVE_LONG,
				3, new TypeSignature("long"), WeiwoDBType.LONG));
		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(columns);
		WeiwoQueryService handler = new WeiwoQueryService(reader, cursor);
		try {
			handler.build(pushdown);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}

		// while (collector.hasNext()) {
		// System.out.println(collector.getSortedDocValues(1) + " "
		// + collector.getSortedDocValues(2));
		// }
	}

	@After
	public void tearDown() {
		if (reader != null) {
			try {
				reader.close();
			} catch (IOException e) {
				Assert.fail();
			}
		}
	}

	private PushDown getPushDown(Expression exp, FunctionCall[] functionCall,
			Symbol[] groupBySymbol) {

		if (exp != null && groupBySymbol != null && functionCall != null) {
			Pair expressionPair = new SimpleFilterPair(exp);
			List<OutputSignature> functions = new ArrayList<OutputSignature>();
			for (FunctionCall single : functionCall) {
				Signature sign = new Signature(single.getName().getSuffix(),
						FunctionKind.AGGREGATE,
						new ArrayList<TypeVariableConstraint>(),
						new ArrayList<LongVariableConstraint>(),
						new TypeSignature("bigint"),
						new ArrayList<TypeSignature>(), false);

				functions.add(new OutputSignature(
						new Symbol(single.getName().getSuffix()), 1));
			}

			Pair aggregationPair = new AggregationPair(functions);

			Type[] types = new Type[groupBySymbol.length];

			for (int i = 0; i < groupBySymbol.length; i++) {
				types[i] = VarcharType.VARCHAR;
			}
			Pair groupByPair = new GroupByPair(Stream.of(groupBySymbol)
					.map(item -> new OutputSignature(item, 1))
					.collect(Collectors.toList()));
			return new PushDown(expressionPair, aggregationPair, groupByPair,
					null);
		}

		if (exp != null && functionCall != null) {
			Pair expressionPair = new SimpleFilterPair(exp);
			List<OutputSignature> functions = new ArrayList<OutputSignature>();
			for (FunctionCall single : functionCall) {
				Signature sign = new Signature(single.getName().getSuffix(),
						FunctionKind.AGGREGATE,
						new ArrayList<TypeVariableConstraint>(),
						new ArrayList<LongVariableConstraint>(),
						new TypeSignature("bigint"),
						new ArrayList<TypeSignature>(), false);

				functions.add(new OutputSignature(
						new Symbol(single.getName().getSuffix()), 1));
			}

			Pair aggregationPair = new AggregationPair(functions);
			return new PushDown(expressionPair, aggregationPair, null, null);
		}

		if (exp != null && groupBySymbol != null) {
			Pair expressionPair = new SimpleFilterPair(exp);

			Pair groupByPair = new GroupByPair(Stream.of(groupBySymbol)
					.map(item -> new OutputSignature(item, 1))
					.collect(Collectors.toList()));
			return new PushDown(expressionPair, null, groupByPair, null);
		}

		if (exp != null) {
			Pair expressionPair = new SimpleFilterPair(exp);
			return new PushDown(expressionPair, null, null, null);
		}

		return new PushDown(null, null, null, null);
	}

	private FunctionCall[] getFunctionCall(String sql) {
		SqlParser parser = new SqlParser();
		Statement statement = parser.createStatement(sql);
		com.facebook.presto.sql.tree.Query query = (com.facebook.presto.sql.tree.Query) statement;
		QuerySpecification querybody = (QuerySpecification) query
				.getQueryBody();
		List<FunctionCall> functionCall = new ArrayList<FunctionCall>();
		for (SelectItem item : querybody.getSelect().getSelectItems()) {
			SingleColumn singleColumn = (SingleColumn) item;
			if (singleColumn.getExpression() instanceof FunctionCall) {
				FunctionCall function = (FunctionCall) singleColumn
						.getExpression();
				functionCall.add(function);
			}
		}
		FunctionCall[] functions = functionCall.toArray(new FunctionCall[0]);
		return functions.length == 0 ? null : functions;
	}

	private Symbol[] getGroupBy(String sql) {
		SqlParser parser = new SqlParser();
		Statement statement = parser.createStatement(sql);
		com.facebook.presto.sql.tree.Query query = (com.facebook.presto.sql.tree.Query) statement;
		QuerySpecification querybody = (QuerySpecification) query
				.getQueryBody();
		Optional<GroupBy> optional = querybody.getGroupBy();
		if (optional.isPresent()) {
			List<Symbol> symbols = new ArrayList<Symbol>();
			for (GroupingElement groupbyElement : optional.get()
					.getGroupingElements()) {
				QualifiedNameReference refer = (QualifiedNameReference) groupbyElement
						.enumerateGroupingSets().get(0)
						.toArray(new Expression[0])[0];
				symbols.add(new Symbol(refer.getName().getSuffix()));
			}
			return symbols.toArray(new Symbol[0]);
		} else {
			return null;
		}
	}

	private Expression getWhereExp(String sql) {
		SqlParser parser = new SqlParser();
		Statement statement = parser.createStatement(sql);
		com.facebook.presto.sql.tree.Query query = (com.facebook.presto.sql.tree.Query) statement;

		QuerySpecification querybody = (QuerySpecification) query
				.getQueryBody();

		Expression expression = null;

		if (!querybody.getWhere().isPresent()) {
			expression = BooleanLiteral.TRUE_LITERAL;

		} else {
			expression = querybody.getWhere().get();
		}
		return expression;
	}

}
