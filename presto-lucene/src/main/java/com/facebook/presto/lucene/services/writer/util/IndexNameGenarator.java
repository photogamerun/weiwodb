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
package com.facebook.presto.lucene.services.writer.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;

public class IndexNameGenarator {

	private static AtomicLong ac = new AtomicLong(0);
	private static String link = "_";

	private IndexNameGenarator() {
	}

	public static String genarateIndexName() {
		StringBuilder sb = new StringBuilder();
		sb.append(WeiwoDBConfigureKeys.INDEX_NAME_PREFIX);
		sb.append(new SimpleDateFormat("yyyyMMddHHmmss")
				.format(new Date(System.currentTimeMillis())));
		sb.append(link);
		sb.append(ac.getAndIncrement());
		return sb.toString();
	}

}
