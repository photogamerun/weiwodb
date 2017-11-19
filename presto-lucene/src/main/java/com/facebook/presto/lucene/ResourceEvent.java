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
package com.facebook.presto.lucene;

import java.util.EventObject;

/**
 * 
 * @author peter.wei
 *
 */
public class ResourceEvent extends EventObject {

	private static final long serialVersionUID = 1L;

	public static final int TYPE_CONNECTED = 1;
	public static final int TYPE_DISCONNECTED = 0;

	private int status = TYPE_DISCONNECTED;

	public ResourceEvent(Object source, int status) {
		super(source);
		this.status = status;
	}

	public int getStatus() {
		return status;
	}
}