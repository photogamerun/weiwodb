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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;

import com.facebook.presto.lucene.WeiwoDBType;
import com.facebook.presto.sql.tree.ComparisonExpression.Type;
/**
 * @author peter.wei
 */
public class ComprisonTranslateFactory extends AbstractTranslateFactory {

	private static Map<String, LiteralTranslation> tranlations = new HashMap<String, LiteralTranslation>();

	Type type;

	static {
		tranlations.put(WeiwoDBType.LONG, LiteralTranslation.LONG);
		tranlations.put(WeiwoDBType.DOUBLE, LiteralTranslation.DOUBLE);
		tranlations.put(WeiwoDBType.INT, LiteralTranslation.INT);
		tranlations.put(WeiwoDBType.FLOAT, LiteralTranslation.FLOAT);
		tranlations.put(WeiwoDBType.STRING, LiteralTranslation.STRING);
	}

	ComprisonTranslateFactory(Type type) {
		this.type = type;
	}

	@Override
	Query toQuery(Object refer, Object literal, String luceneType) {
		return tranlations.get(luceneType).translate(String.valueOf(refer),
				literal, type);
	}

	private enum LiteralTranslation {

		INT {
			@Override
			public Query toEqual(String key, Object value) {
				return IntPoint.newExactQuery(key, (Integer) value);
			}

			@Override
			public Query toNotEqual(String key, Object value) {
				BooleanQuery.Builder builder = new BooleanQuery.Builder();
				builder.add(new MatchAllDocsQuery(), Occur.MUST);
				builder.add(IntPoint.newExactQuery(key, (Integer) value),
						Occur.MUST_NOT);
				return builder.build();
			}

			@Override
			public Query toLessThan(String key, Object value) {
				return IntPoint.newRangeQuery(key, Integer.MIN_VALUE,
						Math.decrementExact((Integer) value));
			}

			@Override
			public Query toLessThanOrEqual(String key, Object value) {
				return IntPoint.newRangeQuery(key, Integer.MIN_VALUE,
						((Integer) value));
			}

			@Override
			public Query toGreaterThanOrEqual(String key, Object value) {
				return IntPoint.newRangeQuery(key, ((Integer) value),
						Integer.MAX_VALUE);
			}

			@Override
			public Query toGreaterThan(String key, Object value) {
				return IntPoint.newRangeQuery(key,
						Math.incrementExact((Integer) value),
						Integer.MAX_VALUE);
			}
		},

		LONG {

			@Override
			public Query toEqual(String key, Object value) {
				return LongPoint.newExactQuery(key, (Long) value);
			}

			@Override
			public Query toNotEqual(String key, Object value) {
				BooleanQuery.Builder builder = new BooleanQuery.Builder();
				builder.add(new MatchAllDocsQuery(), Occur.MUST);
				builder.add(LongPoint.newExactQuery(key, (Long) value),
						Occur.MUST_NOT);
				return builder.build();
			}

			@Override
			public Query toLessThan(String key, Object value) {
				return LongPoint.newRangeQuery(key, Long.MIN_VALUE,
						Math.decrementExact((Long) value));
			}

			@Override
			public Query toLessThanOrEqual(String key, Object value) {
				return LongPoint.newRangeQuery(key, Long.MIN_VALUE,
						((Long) value));
			}

			@Override
			public Query toGreaterThanOrEqual(String key, Object value) {
				return LongPoint.newRangeQuery(key, ((Long) value),
						Long.MAX_VALUE);
			}

			@Override
			public Query toGreaterThan(String key, Object value) {
				return LongPoint.newRangeQuery(key,
						Math.incrementExact((Long) value), Long.MAX_VALUE);
			}
		},

		DOUBLE {

			@Override
			public Query toEqual(String key, Object value) {
				return DoublePoint.newExactQuery(key, (Double) value);
			}

			@Override
			public Query toNotEqual(String key, Object value) {
				BooleanQuery.Builder builder = new BooleanQuery.Builder();
				builder.add(new MatchAllDocsQuery(), Occur.MUST);
				builder.add(DoublePoint.newExactQuery(key, (Double) value),
						Occur.MUST_NOT);
				return builder.build();
			}

			@Override
			public Query toLessThan(String key, Object value) {
				return DoublePoint.newRangeQuery(key, Double.MIN_VALUE,
						Math.nextDown((Double) value));
			}

			@Override
			public Query toLessThanOrEqual(String key, Object value) {
				return DoublePoint.newRangeQuery(key, Double.MIN_VALUE,
						((Double) value));
			}

			@Override
			public Query toGreaterThanOrEqual(String key, Object value) {
				return DoublePoint.newRangeQuery(key, ((Double) value),
						Double.MAX_VALUE);
			}

			@Override
			public Query toGreaterThan(String key, Object value) {
				return DoublePoint.newRangeQuery(key,
						Math.nextUp((Double) value), Double.MAX_VALUE);
			}
		},

		FLOAT {

			@Override
			public Query toEqual(String key, Object value) {
				return FloatPoint.newExactQuery(key, Float.valueOf(value + ""));
			}

			@Override
			public Query toNotEqual(String key, Object value) {
				BooleanQuery.Builder builder = new BooleanQuery.Builder();
				builder.add(new MatchAllDocsQuery(), Occur.MUST);
				builder.add(FloatPoint.newExactQuery(key,
						Float.valueOf(value + "")), Occur.MUST_NOT);
				return builder.build();
			}

			@Override
			public Query toLessThan(String key, Object value) {
				return FloatPoint.newRangeQuery(key, Float.MIN_VALUE,
						Math.nextDown(Float.valueOf(value + "")));
			}

			@Override
			public Query toLessThanOrEqual(String key, Object value) {
				return FloatPoint.newRangeQuery(key, Float.MIN_VALUE,
						(Float.valueOf(value + "")));
			}

			@Override
			public Query toGreaterThanOrEqual(String key, Object value) {
				return FloatPoint.newRangeQuery(key,
						(Float.valueOf(value + "")), Float.MAX_VALUE);
			}

			@Override
			public Query toGreaterThan(String key, Object value) {
				return FloatPoint.newRangeQuery(key,
						Math.nextUp(Float.valueOf(value + "")),
						Float.MAX_VALUE);
			}
		},

		STRING {

			@Override
			public Query toEqual(String key, Object value) {
				if (String.valueOf(value).endsWith("%")
						|| String.valueOf(value).startsWith("%")) {
					return AbstractTranslateFactory.getLikeTranslateFactory()
							.toQuery(key, value, WeiwoDBType.STRING);
				} else {
					if ("vpartition".equalsIgnoreCase(key)) {
						return new MatchAllDocsQuery();
					}
					return new TermQuery(new Term(key, String.valueOf(value)));
				}
			}

			@Override
			public Query toNotEqual(String key, Object value) {
				if (value == null) {
					value = "";
				}
				BooleanQuery.Builder builder = new BooleanQuery.Builder();
				builder.add(new MatchAllDocsQuery(), Occur.MUST);
				builder.add(new TermQuery(new Term(key, String.valueOf(value))),
						Occur.MUST_NOT);
				return builder.build();
			}

			@Override
			public Query toLessThan(String key, Object value) {
				return TermRangeQuery.newStringRange(key, null,
						String.valueOf(value), false, false);
			}

			@Override
			public Query toLessThanOrEqual(String key, Object value) {
				return TermRangeQuery.newStringRange(key, null,
						String.valueOf(value), false, true);
			}

			@Override
			public Query toGreaterThanOrEqual(String key, Object value) {
				return TermRangeQuery.newStringRange(key, String.valueOf(value),
						null, true, false);
			}

			@Override
			public Query toGreaterThan(String key, Object value) {
				return TermRangeQuery.newStringRange(key, String.valueOf(value),
						null, false, false);
			}
		};

		public Query translate(String key, Object value, Type type) {
			switch (type) {
				case EQUAL :
					return toEqual(key, value);
				case NOT_EQUAL :
					return toNotEqual(key, value);
				case LESS_THAN :
					return toLessThan(key, value);
				case LESS_THAN_OR_EQUAL :
					return toLessThanOrEqual(key, value);
				case GREATER_THAN :
					return toGreaterThan(key, value);
				case GREATER_THAN_OR_EQUAL :
					return toGreaterThanOrEqual(key, value);
				default :
					return null;
			}
		}

		public abstract Query toEqual(String key, Object value);

		public abstract Query toNotEqual(String key, Object value);

		public abstract Query toLessThan(String key, Object value);

		public abstract Query toLessThanOrEqual(String key, Object value);

		public abstract Query toGreaterThanOrEqual(String key, Object value);

		public abstract Query toGreaterThan(String key, Object value);
	}
}