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
package com.facebook.presto.util;

import org.junit.Assert;
import org.junit.Test;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Statement;

public class PushDownUtilitiesTest {

	@Test
	public void testPushDownLogic() {

		Expression expression = getWhereExp(
				"select sum(price) from name a where c in ('123','234','456') and b =2 and d>120");
		Assert.assertTrue(PushDownUtilities.isPushDownFilter(expression));
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

	@Test
	public void testIsPushDownCheck() {
		Expression expression = getWhereExp(
				"select * from weiwop8 where vpartition = '1' order by path");
		Assert.assertTrue(PushDownUtilities.isPushDownFilter(expression));
	}

	@Test
	public void testJoinQueryCheckout() {
		Expression expression = getWhereExp(
				"select * from weiwop8 b left join weiwop7 a on b.key=a.key where b.vpartition = '1' order by a.path");

		for (String partition : PushDownUtilities
				.extractPartition(expression)) {
			System.out.println(partition);
		}
	}

	@Test
	public void extractParitionInfo_equals() {
		Expression expression = getWhereExp(
				"select sum(price) from name a where c in ('123','234','456') and vpartition = '2' and d>120");
		String[] parition = PushDownUtilities.extractPartition(expression);
		Assert.assertEquals(1, parition.length);
		Assert.assertEquals("2", parition[0]);
	}

	@Test
	public void extractParitionInfo_empty() {
		Expression expression = BooleanLiteral.TRUE_LITERAL;
		String[] parition = PushDownUtilities.extractPartition(expression);
		Assert.assertEquals(0, parition.length);
	}

	// select * from weiwo_type3 where vpartition='1' and birthday =
	// cast('1970-01-01' as timestamp);

	@Test
	public void extractPartitionInfo_cast() {
		Expression expression = getWhereExp(
				"select * from weiwo_type3 where vpartition='1' and birthday = cast('1970-01-01' as timestamp)");
		String[] parition = PushDownUtilities.extractPartition(expression);
		Assert.assertEquals(1, parition.length);
	}

	@Test
	public void fail_to_extractParitionInfo_lowthan() {
		Expression expression = getWhereExp(
				"select sum(price) from name a where c in ('123','234','456') and vpartition > '2' and d>120");
		String[] parition = PushDownUtilities.extractPartition(expression);
		Assert.assertEquals(0, parition.length);

	}

	@Test
	public void testIsPushDownFilter() {
		boolean isPushDown = PushDownUtilities
				.isPushDownFilter(BooleanLiteral.TRUE_LITERAL);
		Assert.assertTrue(isPushDown);
	}

	@Test
	public void extractParitionInfo_inPredicated() {
		Expression expression = getWhereExp(
				"select sum(price) from name a where c in ('123','234','456') and vpartition in ('2','3','4') and d>120");

		String[] parition = PushDownUtilities.extractPartition(expression);
		Assert.assertEquals(3, parition.length);
		Assert.assertEquals("2", parition[0]);
		Assert.assertEquals("3", parition[1]);
		Assert.assertEquals("4", parition[2]);
	}
}