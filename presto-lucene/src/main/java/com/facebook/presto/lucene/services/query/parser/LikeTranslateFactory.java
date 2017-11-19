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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;

public class LikeTranslateFactory extends AbstractTranslateFactory {

	LikeTranslateFactory() {

	}

	@Override
	Query toQuery(Object refer, Object literal,String luceneType) {
		Term term = new Term(String.valueOf(refer),
				toWildcard(String.valueOf(literal)));
		WildcardQuery wildCard = new WildcardQuery(term);
		return wildCard;
	}

	private String toWildcard(String wholeWord) {
		return wholeWord.replace("%", "*");
	}
}