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
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;

import com.facebook.presto.lucene.WeiwoDBType;

public class BetweenTranslateFactory extends AbstractTranslateFactory {

	public static Map<String, BetweenTranslation> translations = new HashMap<String, BetweenTranslation>();

	static {
		translations.put(WeiwoDBType.LONG, BetweenTranslation.LONG);
		translations.put(WeiwoDBType.INT, BetweenTranslation.INT);
		translations.put(WeiwoDBType.DOUBLE, BetweenTranslation.DOUBLE);
		translations.put(WeiwoDBType.FLOAT, BetweenTranslation.FLOAT);
		translations.put(WeiwoDBType.STRING, BetweenTranslation.STRING);
	}

	@Override
	Query toQuery(Object refer, Object literal, String luceneType) {
		BetweenTranslation inpredicate = translations.get(luceneType);
		if (inpredicate != null) {
			Object[] values = (Object[]) literal;
			return inpredicate.translate((String) refer, values[0], values[1]);
		} else {
			return new MatchNoDocsQuery();
		}
	}

	private static enum BetweenTranslation {

		INT {
			@Override
			public Query translate(String key, Object left, Object right) {
				return IntPoint.newRangeQuery(key, ((Long) left).intValue(),
						((Long) right).intValue());
			}
		},

		FLOAT {
			@Override
			public Query translate(String key, Object left, Object right) {
				return FloatPoint.newRangeQuery(key, Float.valueOf(left + ""),
						Float.valueOf(right + ""));
			}
		},

		LONG {
			@Override
			public Query translate(String key, Object left, Object right) {
				return LongPoint.newRangeQuery(key, (Long) left, (Long) right);
			}
		},

		DOUBLE {
			@Override
			public Query translate(String key, Object left, Object right) {
				return DoublePoint.newRangeQuery(key, (Double) left,
						(Double) right);
			}
		},

		STRING {
			@Override
			public Query translate(String key, Object left, Object right) {
				return TermRangeQuery.newStringRange(key, (String) left,
						(String) right, true, true);
			}
		};

		/**
		 * 
		 * @param left:
		 *            lower portion of the range (inclusive).
		 * @param right:
		 *            upper portion of the range (inclusive).
		 * @param key:
		 *            field name
		 * @return
		 */
		public abstract Query translate(String key, Object left, Object right);
	}
}
