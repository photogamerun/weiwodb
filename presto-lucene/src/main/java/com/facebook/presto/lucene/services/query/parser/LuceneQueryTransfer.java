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
package com.facebook.presto.lucene.services.query.parser;

import static java.util.Objects.requireNonNull;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import com.facebook.presto.lucene.LuceneRecordCursor;
import com.facebook.presto.lucene.WeiwoDBType;
import com.facebook.presto.spi.pushdown.QueryTransfer;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.util.PushDownUtilities;
import com.google.common.base.Throwables;

import io.airlift.log.Logger;

/**
 * Convert standard SQL to LUCENE query
 * 
 * push down the filter logic from Presto to data source layer.
 * 
 * precondition:
 * 
 * data source layer supports query function.
 * 
 * this class only for LUCENE filter logic push down.
 * 
 * @author peter.wei
 *
 */
public class LuceneQueryTransfer implements QueryTransfer<Expression, Query> {

	private static final Logger log = Logger.get(LuceneQueryTransfer.class);

	PushDownVisiter visiter = new PushDownVisiter();

	private LuceneRecordCursor curser;

	public LuceneQueryTransfer(LuceneRecordCursor curser) {
		this.curser = requireNonNull(curser, "curser is null");;
	}

	@Override
	public Query transfer(Expression express) {

		if (express == null || express == BooleanLiteral.TRUE_LITERAL
				|| !PushDownUtilities.isPushDownFilter(express)) {
			return new MatchAllDocsQuery();
		} else {
			Query query = handlePushdown(express);
			return query;
		}
	}

	private Query handlePushdown(Expression filter) {
		return (Query) visiter.process(filter, null);
	}

	public class PushDownVisiter extends AstVisitor<Object, Map> {

		@Override
		protected Query visitComparisonExpression(ComparisonExpression node,
				Map context) {
			Object left = process(node.getLeft(), context);
			Object right = process(node.getRight(), context);
			if (left.getClass() == Long.class && (left == right)) {
				return new MatchAllDocsQuery();
			} else {
				// 处理非数值类型的double 和 float
				String luceneType = curser.getLuceneType(String.valueOf(left));
				if (Objects.equals(WeiwoDBType.FLOAT, luceneType)
						&& Objects.equals("NaN", right)) {
					return AbstractTranslateFactory
							.getCompareTranslateFatory(node.getType())
							.toQuery(left, Float.NaN, luceneType);
				} else if (Objects.equals(WeiwoDBType.DOUBLE, luceneType)
						&& Objects.equals("NaN", right)) {
					return AbstractTranslateFactory
							.getCompareTranslateFatory(node.getType())
							.toQuery(left, Double.NaN, luceneType);
				} else if (Objects.equals(WeiwoDBType.INT, luceneType)) {

					return AbstractTranslateFactory
							.getCompareTranslateFatory(node.getType())
							.toQuery(left, ((Long) right).intValue(),
									luceneType);
				} else {
					return AbstractTranslateFactory
							.getCompareTranslateFatory(node.getType())
							.toQuery(left, right, luceneType);
				}
			}

		}

		@Override
		protected Object visitStringLiteral(StringLiteral node, Map context) {
			return node.getValue();
		}
		@Override
		protected Object visitGenericLiteral(GenericLiteral node, Map context) {
			if ("BigInt".equalsIgnoreCase(node.getType())) {
				return Long.parseLong(node.getValue());
			} else {
				return node.getValue();
			}
		}

		@Override
		protected Object visitDoubleLiteral(DoubleLiteral node, Map context) {
			return node.getValue();
		}

		@Override
		protected Object visitBooleanLiteral(BooleanLiteral node, Map context) {
			return node.getValue();
		}

		@Override
		protected Object visitCast(Cast node, Map context) {
			Object value = process(node.getExpression(), context);
			if (value == null) {
				throw Throwables.propagate(
						new IllegalArgumentException("not support =,>,<,null "
								+ node + " please use is null or is not null"));
			}

			if ("TIMESTAMP".equals(node.getType())) {
				String strVal = String.valueOf(value);
				Date data;
				try {
					String format;
					if (strVal.length() == 10) {
						format = "yyyy-MM-dd";
					} else {
						if (strVal.length() == "yyyy-MM-dd HH:mm:ss".length()) {
							format = "yyyy-MM-dd HH:mm:ss";
						} else {
							format = "yyyy-MM-dd HH:mm:ss.SSS";
						}
					}
					SimpleDateFormat dataformat = new SimpleDateFormat(format);
					data = dataformat.parse(strVal);
					long time = data.getTime();
					return time;
				} catch (ParseException e) {
					log.error(e,
							"unsupport date format in weiwodb please refer format as yyyy-MM-dd HH:mm:ss");
					throw Throwables.propagate(e);
				}
			} else {
				return value;
			}
		}

		@Override
		protected Object visitQualifiedNameReference(
				QualifiedNameReference node, Map context) {
			return node.getName().getOriginalParts().get(0);
		}

		@Override
		protected Object visitLongLiteral(LongLiteral node, Map context) {
			return node.getValue();
		}

		@Override
		protected Query visitLogicalBinaryExpression(
				LogicalBinaryExpression node, Map context) {
			BooleanQuery.Builder builder = new BooleanQuery.Builder();
			Object queryLeft = this.process(node.getLeft(), context);
			Object queryReight = this.process(node.getRight(), context);
			if (queryReight == null || queryLeft == null) {
				throw Throwables.propagate(new IllegalArgumentException(
						"not support >null，<null, in(null), not in(null) in "
								+ node));
			}
			switch (node.getType()) {
				case AND :
					builder.add((Query) queryLeft, Occur.MUST)
							.add((Query) queryReight, Occur.MUST);
					return builder.build();
				case OR :
					builder.add((Query) queryLeft, Occur.SHOULD)
							.add((Query) queryReight, Occur.SHOULD);
					return builder.build();
				default :
					log.error("doesn't support right now");
			}
			return builder.build();
		}

		@Override
		protected Object visitSymbolReference(SymbolReference node,
				Map context) {
			return node.getName();
		}

		/**
		 * @param lowerValue
		 *            lower portion of the range (inclusive).
		 * @param upperValue
		 *            upper portion of the range (inclusive).
		 * 
		 */
		@Override
		protected Object visitBetweenPredicate(BetweenPredicate node,
				Map context) {
			Object max = process(node.getMax(), context);
			Object min = process(node.getMin(), context);
			Object key = process(node.getValue(), context);
			String luceneType = LuceneQueryTransfer.this.curser
					.getLuceneType((String) key);
			return AbstractTranslateFactory.getBetweenTranslateFactory()
					.toQuery(key, new Object[]{min, max}, luceneType);
		}

		@Override
		protected Object visitLikePredicate(LikePredicate node, Map context) {
			String key = (String) process(node.getValue(), context);
			String value = (String) process(node.getPattern(), context);
			return AbstractTranslateFactory.getLikeTranslateFactory()
					.toQuery(key, value, WeiwoDBType.STRING);
		}

		@Override
		protected Object visitInPredicate(InPredicate node, Map context) {
			String name = (String) process(node.getValue(), context);
			Object[] values = (Object[]) process(node.getValueList(), context);
			if (values.length > 0) {
				Object firstValue = values[0];
				// 处理 in 语句中的null 值
				if (firstValue == null) {
					return AbstractTranslateFactory
							.getInPredicateTranslateFactory()
							.toQuery(name, values, WeiwoDBType.STRING);
				} else {
					String luceneType = curser.getLuceneType(name);
					return AbstractTranslateFactory
							.getInPredicateTranslateFactory()
							.toQuery(name, values, luceneType);
				}
			} else {
				log.error("didn't fill in set query before publishing");
				return null;
			}
		}

		@Override
		protected Object[] visitInListExpression(InListExpression node,
				Map context) {
			List<Object> values = new LinkedList<Object>();
			for (Expression express : node.getValues()) {
				values.add(process(express, context));
			}
			return values.toArray();
		}

		@Override
		protected Object visitNotExpression(NotExpression node, Map context) {
			Query expression = (Query) this.process(node.getValue(), context);
			BooleanQuery.Builder builder = new BooleanQuery.Builder();
			builder.add(new MatchAllDocsQuery(), Occur.MUST);
			builder.add(expression, Occur.MUST_NOT);
			return builder.build();
		}

		@Override
		protected Object visitIsNotNullPredicate(IsNotNullPredicate node,
				Map context) {
			String key = (String) process(node.getValue(), context);
			String luceneType = curser.getLuceneType(key);

			if (WeiwoDBType.DOUBLE == luceneType) {
				return AbstractTranslateFactory.getBetweenTranslateFactory()
						.toQuery(key, new Object[]{Double.MIN_VALUE,
								Double.MAX_VALUE}, luceneType);
			}
			if (WeiwoDBType.FLOAT == luceneType) {
				return AbstractTranslateFactory.getBetweenTranslateFactory()
						.toQuery(key,
								new Object[]{Float.MIN_VALUE, Float.MAX_VALUE},
								luceneType);
			}
			if (WeiwoDBType.STRING == luceneType) {
				return AbstractTranslateFactory.getLikeTranslateFactory()
						.toQuery(key, "*", luceneType);
			}
			if (WeiwoDBType.INT == luceneType) {
				return AbstractTranslateFactory.getBetweenTranslateFactory()
						.toQuery(key, new Object[]{Integer.MIN_VALUE,
								Integer.MAX_VALUE}, luceneType);
			}
			if (WeiwoDBType.LONG == luceneType) {
				return AbstractTranslateFactory.getBetweenTranslateFactory()
						.toQuery(key,
								new Object[]{Long.MIN_VALUE, Long.MAX_VALUE},
								luceneType);
			}
			if (WeiwoDBType.TEXT == luceneType) {
				return AbstractTranslateFactory.getLikeTranslateFactory()
						.toQuery(key, "*", luceneType);
			}
			if (WeiwoDBType.TIMESTAMP == luceneType) {
				return AbstractTranslateFactory.getBetweenTranslateFactory()
						.toQuery(key,
								new Object[]{Long.MIN_VALUE, Long.MAX_VALUE},
								WeiwoDBType.LONG);
			} else {
				throw Throwables.propagate(new IllegalArgumentException(
						"not support null value for type " + luceneType));
			}
		}

		@Override
		protected Object visitFunctionCall(FunctionCall node, Map context) {
			List<Expression> args = node.getArguments();
			if (args.size() > 0) {
				return this.process(node.getArguments().get(0), context);
			} else {
				return Throwables.propagate(new IllegalArgumentException(
						"only support one argument in arg list"));
			}
		}

		@Override
		protected Object visitTimestampLiteral(TimestampLiteral node,
				Map context) {
			return node.getValue();
		}

		@Override
		protected Object visitIsNullPredicate(IsNullPredicate node,
				Map context) {
			String key = (String) process(node.getValue(), context);
			String luceneType = curser.getLuceneType(key);
			if (Objects.equals(WeiwoDBType.DOUBLE, luceneType)) {
				BooleanQuery.Builder builder = new BooleanQuery.Builder();
				builder.add(new MatchAllDocsQuery(), Occur.MUST);
				builder.add(
						AbstractTranslateFactory.getBetweenTranslateFactory()
								.toQuery(key,
										new Object[]{Double.MIN_VALUE,
												Double.MAX_VALUE},
										luceneType),
						Occur.MUST_NOT);
				return builder.build();
			}
			if (Objects.equals(WeiwoDBType.FLOAT, luceneType)) {
				BooleanQuery.Builder builder = new BooleanQuery.Builder();
				builder.add(new MatchAllDocsQuery(), Occur.MUST);
				builder.add(
						AbstractTranslateFactory.getBetweenTranslateFactory()
								.toQuery(key,
										new Object[]{Float.MIN_VALUE,
												Float.MAX_VALUE},
										luceneType),
						Occur.MUST_NOT);
				return builder.build();
			}
			if (Objects.equals(WeiwoDBType.STRING, luceneType)) {
				BooleanQuery.Builder builder = new BooleanQuery.Builder();
				builder.add(new MatchAllDocsQuery(), Occur.MUST);
				builder.add(
						AbstractTranslateFactory.getLikeTranslateFactory()
								.toQuery(key, "*", WeiwoDBType.STRING),
						Occur.MUST_NOT);
				return builder.build();
			}
			if (Objects.equals(WeiwoDBType.INT, luceneType)) {
				BooleanQuery.Builder builder = new BooleanQuery.Builder();
				builder.add(new MatchAllDocsQuery(), Occur.MUST);
				builder.add(
						AbstractTranslateFactory.getBetweenTranslateFactory()
								.toQuery(key,
										new Object[]{Integer.MIN_VALUE,
												Integer.MAX_VALUE},
										luceneType),
						Occur.MUST_NOT);
				return builder.build();
			}
			if (Objects.equals(WeiwoDBType.LONG, luceneType)) {
				BooleanQuery.Builder builder = new BooleanQuery.Builder();
				builder.add(new MatchAllDocsQuery(), Occur.MUST);
				builder.add(
						AbstractTranslateFactory.getBetweenTranslateFactory()
								.toQuery(key,
										new Object[]{Long.MIN_VALUE,
												Long.MAX_VALUE},
										luceneType),
						Occur.MUST_NOT);
				return builder.build();
			}
			if (Objects.equals(WeiwoDBType.TEXT, luceneType)) {
				BooleanQuery.Builder builder = new BooleanQuery.Builder();
				builder.add(new MatchAllDocsQuery(), Occur.MUST);
				builder.add(
						AbstractTranslateFactory.getLikeTranslateFactory()
								.toQuery(key, "*", WeiwoDBType.STRING),
						Occur.MUST_NOT);
				return builder.build();
			}
			if (Objects.equals(WeiwoDBType.TIMESTAMP, luceneType)) {
				BooleanQuery.Builder builder = new BooleanQuery.Builder();
				builder.add(new MatchAllDocsQuery(), Occur.MUST);
				builder.add(AbstractTranslateFactory
						.getBetweenTranslateFactory().toQuery(key,
								new Object[]{Long.MIN_VALUE, Long.MAX_VALUE},
								WeiwoDBType.LONG),
						Occur.MUST_NOT);
				return builder.build();
			} else {
				throw Throwables.propagate(new IllegalArgumentException(
						"not support null value for type " + luceneType));
			}
		}
	}
}