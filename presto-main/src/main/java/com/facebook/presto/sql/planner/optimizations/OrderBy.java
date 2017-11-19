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
package com.facebook.presto.sql.planner.optimizations;

import static java.util.Objects.requireNonNull;

import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
/**
 * 
 * @author peter.wei push down order by information into table scan node
 */
public class OrderBy {

	private Symbol name;

	private String sortorder;

	@JsonCreator
	public OrderBy(@JsonProperty("name") Symbol name,
			@JsonProperty("sortorder") String sortorder) {
		this.name = requireNonNull(name, "name is null");
		this.sortorder = requireNonNull(sortorder, "sortorder is null");
	}

	@JsonProperty("name")
	public Symbol getName() {
		return name;
	}

	@JsonProperty("sortorder")
	public String getSortorder() {
		return sortorder;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("name", name)
				.add("sortorder", sortorder).toString();
	}
}
