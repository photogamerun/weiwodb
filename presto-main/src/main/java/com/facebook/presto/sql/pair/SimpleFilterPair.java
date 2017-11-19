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
package com.facebook.presto.sql.pair;

import javax.annotation.Nullable;

import com.facebook.presto.spi.pushdown.Pair;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SimpleFilterPair implements Pair<Expression> {

	private Expression expression;

	private static final String FILTER = "filter";

	@JsonCreator
	public SimpleFilterPair(
			@JsonProperty("expression") @Nullable Expression expression) {
		super();
		this.expression = expression;
	}

	@JsonProperty
	public Expression getExpression() {
		return expression;
	}

	@Override
	public Expression getValue() {
		return getExpression();
	}

	@Override
	public String getName() {
		return FILTER;
	}
}