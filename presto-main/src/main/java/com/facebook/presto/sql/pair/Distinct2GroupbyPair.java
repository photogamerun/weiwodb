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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Distinct2GroupbyPair implements Pair<OutputSignature> {

	private static final String DISTINCT2GROUPBY = "distinct2groupby";

	private final OutputSignature distinct2groupby;

	@JsonCreator
	public Distinct2GroupbyPair(
			@JsonProperty("distinct2groupby") @Nullable OutputSignature distinct2groupby) {
		this.distinct2groupby = distinct2groupby;
	}

	@Override
	public OutputSignature getValue() {
		return distinct2groupby;
	}

	@Override
	public String getName() {
		return DISTINCT2GROUPBY;
	}
}