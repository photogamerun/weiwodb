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
package com.facebook.presto.lucene.services.query.internals;

import com.facebook.presto.lucene.services.query.Aggregation;
import com.facebook.presto.sql.pair.OutputSignature;
import com.google.common.base.Throwables;
/**
 * 
 * 
 * @author peter.wei
 *
 */
class AggregationMediator {

	private AggregationMediator() {
	}

	static Aggregation mediate(OutputSignature aggregationName) {
		switch (aggregationName.getKind()) {
			case AGGREAT :
				String name = aggregationName.getName();
				if (name.contains("count")) {
					return new DocNumAggregation(aggregationName);
				}
			default :
				throw Throwables.propagate(new IllegalArgumentException(
						"not support function so far " + aggregationName));
		}
	}
}