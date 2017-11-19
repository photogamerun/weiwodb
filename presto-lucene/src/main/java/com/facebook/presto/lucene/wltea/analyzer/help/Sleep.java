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
package com.facebook.presto.lucene.wltea.analyzer.help;

import io.airlift.log.Logger;

public class Sleep {

	public static Logger logger = Logger.get(Sleep.class);

	public enum Type {
		MSEC, SEC, MIN, HOUR
	};

	public static void sleep(Type type, int num) {
		try {
			switch (type) {
				case MSEC :
					Thread.sleep(num);
					return;
				case SEC :
					Thread.sleep(num * 1000l);
					return;
				case MIN :
					Thread.sleep(num * 60l * 1000l);
					return;
				case HOUR :
					Thread.sleep(num * 60l * 60l * 1000l);
					return;
				default :
					logger.error("输入类型错误，应为MSEC,SEC,MIN,HOUR之一");
					return;
			}
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
	}

}