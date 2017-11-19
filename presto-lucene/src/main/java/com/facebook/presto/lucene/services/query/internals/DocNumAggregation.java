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

import static com.facebook.presto.lucene.Types.checkType;

import com.facebook.presto.lucene.services.query.Aggregation;
import com.facebook.presto.sql.pair.AggOutputSignature;
import com.facebook.presto.sql.pair.OutputSignature;

class DocNumAggregation extends Aggregation {

	private AggOutputSignature signature;

	private int channel;

	DocNumAggregation(OutputSignature signature) {
		this.signature = checkType(signature, AggOutputSignature.class,
				"agg signature");
		this.channel = signature.getChannel();

	}

	@Override
	public void aggregate(Object[] source) {
		Long v = (Long) source[channel];
		source[channel] = v == null ? 1L : ++v;
	}
}