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

import java.util.List;

import javax.annotation.Nullable;

import com.facebook.presto.spi.pushdown.Pair;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class GroupByPair implements Pair<List<OutputSignature>> {

	private List<OutputSignature> groupby;

	private static final String GROUP = "group";

	@JsonCreator
	public GroupByPair(
			@JsonProperty("groupby") @Nullable List<OutputSignature> groupby) {
		this.groupby = groupby;
	}

	public List<OutputSignature> getGroupby() {
		return groupby;
	}

	@Override
	public List<OutputSignature> getValue() {
		return groupby;
	}

	@Override
	public String getName() {
		return GROUP;
	}
}