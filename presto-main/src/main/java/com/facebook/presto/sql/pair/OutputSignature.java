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

import com.facebook.presto.sql.planner.Symbol;
/**
 * 
 * @author peter.wei
 *
 */
public class OutputSignature {

	Symbol symbol;

	private OutputType kind;

	private int channel;

	public OutputSignature(Symbol symbol, int channel) {
		this(OutputType.FIELD, symbol, channel);
	}

	public OutputSignature(OutputType kind, Symbol symbol, int channel) {
		this.kind = kind;
		this.symbol = symbol;
		this.channel = channel;

	}

	public String getName() {
		return symbol.getName();
	}

	public Symbol getSymbol() {
		return symbol;
	}

	public OutputType getKind() {
		return kind;
	}

	public int getChannel() {
		return channel;
	}

	public enum OutputType {
		AGGREAT, FIELD, HASH, DISTINCT;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + channel;
		result = prime * result + ((kind == null) ? 0 : kind.hashCode());
		result = prime * result + ((symbol == null) ? 0 : symbol.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		OutputSignature other = (OutputSignature) obj;
		if (channel != other.channel) {
			return false;
		}
		if (kind != other.kind) {
			return false;
		}
		if (symbol == null) {
			if (other.symbol != null) {
				return false;
			}
		} else if (!symbol.equals(other.symbol)) {
			return false;
		}
		return true;
	}
}