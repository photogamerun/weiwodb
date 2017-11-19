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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import com.facebook.presto.lucene.WeiwoDBType;
/**
 * 
 * 
 * @author peter.wei
 *
 */
public class InPredicateTranslateFactory extends AbstractTranslateFactory {

	public static Map<String, InPredicateTranslation> translations = new HashMap<String, InPredicateTranslation>();

	static {
		translations.put(WeiwoDBType.LONG, InPredicateTranslation.LONG);
		translations.put(WeiwoDBType.DOUBLE, InPredicateTranslation.DOUBLE);
		translations.put(WeiwoDBType.FLOAT, InPredicateTranslation.FLOAT);
		translations.put(WeiwoDBType.INT, InPredicateTranslation.INT);
		translations.put(WeiwoDBType.STRING, InPredicateTranslation.STRING);
	}

	@Override
	Query toQuery(Object refer, Object literal, String luceneType) {
		InPredicateTranslation inpredicate = translations.get(luceneType);
		if (inpredicate != null) {
			return inpredicate.translate((String) refer, literal);
		} else {
			return new MatchNoDocsQuery();
		}
	}

	private enum InPredicateTranslation {

		INT {

			@Override
			public Query translate(String key, Object value) {
				Object[] objs = (Object[]) value;
				List<Integer> values = new ArrayList<>();
				for (Object obj : objs) {
					values.add(((Long) obj).intValue());
				}
				return IntPoint.newSetQuery(key, values);
			}
		},

		LONG {

			@Override
			public Query translate(String key, Object value) {
				Object[] objs = (Object[]) value;
				List<Long> values = new ArrayList<>();
				for (Object obj : objs) {
					values.add((Long) obj);
				}
				return LongPoint.newSetQuery(key, values);
			}
		},

		FLOAT {

			@Override
			public Query translate(String key, Object value) {
				Object[] objs = (Object[]) value;
				List<Float> values = new ArrayList<>();
				for (Object obj : objs) {
					values.add(Float.valueOf(value + ""));
				}
				return FloatPoint.newSetQuery(key, values);
			}

		},

		/**
		 * refer to http://www.iteye.com/problems/57272
		 * 
		 */
		STRING {

			@Override
			public Query translate(String key, Object value) {
				if ("vpartition".equalsIgnoreCase(key)) {
					return new MatchAllDocsQuery();
				}
				BooleanQuery.Builder builder = new BooleanQuery.Builder();
				Object[] objs = (Object[]) value;
				for (Object obj : objs) {
					builder.add(
							new TermQuery(new Term(key, String.valueOf(obj))),
							Occur.SHOULD);
				}
				return builder.build();
			}
		},
		DOUBLE {

			@Override
			public Query translate(String key, Object value) {
				Object[] objs = (Object[]) value;
				List<Double> values = new ArrayList<>();
				for (Object obj : objs) {
					values.add((Double) obj);
				}
				return DoublePoint.newSetQuery(key, values);
			}
		};

		public abstract Query translate(String key, Object value);
	}
}