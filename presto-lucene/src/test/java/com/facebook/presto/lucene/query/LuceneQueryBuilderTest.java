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
import java.util.Collections;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.facebook.presto.lucene.DummyLuceneRecordCursor;
import com.facebook.presto.lucene.services.query.parser.LuceneQueryTransfer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Statement;

public class LuceneQueryBuilderTest {

	LuceneQueryTransfer builder;

	@Before
	public void setup() throws IOException {
		DummyLuceneRecordCursor cursor = new DummyLuceneRecordCursor(
				Collections.emptyList());
		this.builder = new LuceneQueryTransfer(cursor);
	}

	@Test
	public void testBuildALLPushdown() {
		Query query = builder.transfer(BooleanLiteral.TRUE_LITERAL);
		Assert.assertTrue(query instanceof MatchAllDocsQuery);
	}

	@Test
	public void testBuildEmptyPushDown() {
		Query query = builder.transfer(null);

		Assert.assertTrue(query instanceof MatchAllDocsQuery);
	}

	@Test
	public void testBuildPushDownFilter_lowthangreaterthan() {
		Expression expression = getWhereExp(
				"select * from name a where a > 10 and b <10");
		Query query = builder.transfer(expression);
		Assert.assertTrue(query instanceof BooleanQuery);
		BooleanQuery boq = (BooleanQuery) query;
		Assert.assertEquals(boq.clauses().size(), 2);
		Assert.assertEquals(
				"+a:[11 TO 9223372036854775807] +b:[-9223372036854775808 TO 9]",
				boq.toString());
	}

	@Test
	public void testBuildPushDownFilter_equal() {
		Expression expression = getWhereExp(
				"select * from name a where c='123'");
		Query query = builder.transfer(expression);
		Assert.assertEquals("c:123", query.toString());;
	}

	@Test
	public void testBuildPushDownFilter_InPredicate() {
		Expression expression = getWhereExp(
				"select * from name a where c in ('123','234','456')");
		Query query = builder.transfer(expression);
		Assert.assertEquals("c:123 c:234 c:456", query.toString());
	}

	@Test
	public void testBuildPushDownFileter_MutilFileds() {
		Expression expression = getWhereExp(
				"select * from name a where c in ('123','234','456') and b =2 and d>120");
		Query query = builder.transfer(expression);
		Assert.assertEquals(
				"+(+(c:123 c:234 c:456) +b:[2 TO 2]) +d:[121 TO 9223372036854775807]",
				query.toString());
	}

	@Test
	public void testBuildPushDownFileter_MutilFileds_orCombine() {
		Expression expression = getWhereExp(
				"select * from name a where (b =2 and d>120) and d<3");
		Query query = builder.transfer(expression);
		System.out.println(query.toString());
		Assert.assertEquals(
				"+(+b:[2 TO 2] +d:[121 TO 9223372036854775807]) +d:[-9223372036854775808 TO 2]",
				query.toString());
	}

	@Test
	public void testBuildandSQL() {
		Expression expression = getWhereExp(
				"select  * from weiwodb where name='lucene' and app='solr'");
		Query query = builder.transfer(expression);
		System.out.println(query.toString());
		// select * from table where name=“lucene” and app=“solr”
	}

	@Test
	public void testBuildPushDownFileter_Skip_vpartition() {
		Expression expression = getWhereExp(
				"select a,b from name a where (b =2 and d>120) and vpartition = '3'");
		Query query = builder.transfer(expression);
		Assert.assertFalse((query.toString()).contains("vpartition"));
	}

	@Test
	public void testBuildPushDownFileter_MutilFileds_ordejunction() {
		Expression expression = getWhereExp(
				"select a,b from name a where (b =2 and d>120) or d<3");
		Query query = builder.transfer(expression);
		System.out.println(query.toString());
		Assert.assertEquals(
				"(+b:[2 TO 2] +d:[121 TO 9223372036854775807]) d:[-9223372036854775808 TO 2]",
				query.toString());
	}

	@Test
	public void testBuildPushDownFilter_MutilFileds_ordejunction_notequals() {
		Expression expression = getWhereExp(
				"select a,b from name a where (b =2 and d>120) or d != 3");
		Query query = builder.transfer(expression);
		System.out.println(query.toString());
		Assert.assertEquals(
				"(+b:[2 TO 2] +d:[121 TO 9223372036854775807]) (+*:* -d:[3 TO 3])",
				query.toString());
	}

	@Test
	public void testBuildPushDownFilter_filter_vpartition() {
		Expression expression = getWhereExp(
				"select a,b from name a where (b =2 and d>120) and vpartition='1'");
		Query query = builder.transfer(expression);
		Assert.assertFalse(query.toString().contains("vpartiton"));
	}

	@Test
	public void testBuildPushDownFilter_between() {
		Expression expression = getWhereExp(
				"select a,b from name a where (b =2 and d>120) or d between 1 and 3");
		Query query = builder.transfer(expression);
		System.out.println(query.toString());
	}

	private Expression getWhereExp(String sql) {
		SqlParser parser = new SqlParser();
		Statement statement = parser.createStatement(sql);
		com.facebook.presto.sql.tree.Query query = (com.facebook.presto.sql.tree.Query) statement;

		QuerySpecification querybody = (QuerySpecification) query
				.getQueryBody();

		Expression expression = querybody.getWhere().get();
		return expression;
	}

}
