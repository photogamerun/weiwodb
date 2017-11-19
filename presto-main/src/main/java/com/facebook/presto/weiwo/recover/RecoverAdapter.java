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
package com.facebook.presto.weiwo.recover;

import java.util.Map;

import javax.inject.Inject;

import com.facebook.presto.spi.Recover;
import com.facebook.presto.spi.RecoverAdapterInterface;

import io.airlift.log.Logger;

public class RecoverAdapter implements RecoverAdapterInterface {

	private static final Logger log = Logger.get(RecoverAdapter.class);

	Recover recover;

	@Inject
	public RecoverAdapter() {

	}

	@Override
	public void setRecoverInstance(Recover recover) {
		this.recover = recover;
	}

	@Override
	public boolean startRecover(Map<String, Object> properties) {
		if (recover != null) {
			return recover.startRecover(properties);
		} else {
			log.warn("Recover instance is null.");
			return false;
		}
	}

}
