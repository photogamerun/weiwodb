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

import org.apache.lucene.search.Query;

import com.facebook.presto.sql.tree.ComparisonExpression.Type;
/**
 * 
 * 抽象工厂类，用于处理下推语法的转换：
 * 
 * 具体实现类有：
 * 
 * @see com.facebook.presto.lucene.query。BetweenTranslateFactory，
 * @see com.facebook.presto.lucene.query。ComprisonTranslateFactory，
 * @see com.facebook.presto.lucene.query。InPredicateTranslateFactory
 * @see com.facebook.presto.lucene.query。LikeTranslateFactory
 * 
 * 
 * @author peter.wei
 *
 */
public abstract class AbstractTranslateFactory {

	private static Map<Type, AbstractTranslateFactory> compareFactorys = new HashMap<Type, AbstractTranslateFactory>();

	private static AbstractTranslateFactory inPredicateFactory;

	private static AbstractTranslateFactory betweenFactory;

	private static AbstractTranslateFactory likeFactory;

	public static synchronized AbstractTranslateFactory getCompareTranslateFatory(
			Type type) {
		AbstractTranslateFactory factory = compareFactorys.get(type);
		if (factory == null) {
			factory = new ComprisonTranslateFactory(type);
			compareFactorys.put(type, factory);
		}
		return factory;
	}

	public static synchronized AbstractTranslateFactory getInPredicateTranslateFactory() {
		if (inPredicateFactory == null) {
			inPredicateFactory = new InPredicateTranslateFactory();
		}
		return inPredicateFactory;
	}

	public static synchronized AbstractTranslateFactory getLikeTranslateFactory() {
		if (likeFactory == null) {
			likeFactory = new LikeTranslateFactory();
		}
		return likeFactory;
	}

	public static synchronized AbstractTranslateFactory getBetweenTranslateFactory() {
		if (betweenFactory == null) {
			betweenFactory = new BetweenTranslateFactory();
		}
		return betweenFactory;
	}

	abstract Query toQuery(Object refer, Object literal, String luceneType);

}