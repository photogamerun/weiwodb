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
package com.facebook.presto.lucene.services.writer.internals;

import java.nio.charset.Charset;

import com.facebook.presto.lucene.services.writer.exception.RecoverException;

import io.airlift.log.Logger;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class KafkaDataSourceWriterThread implements Runnable {

	private static final Logger log = Logger
			.get(KafkaDataSourceWriterThread.class);

	private KafkaDataSourceWriter writer;
	private int threadId;
	private KafkaStream mStream;

	public KafkaDataSourceWriterThread(KafkaStream mStream,
			KafkaDataSourceWriter writer, int threadId) {
		this.mStream = mStream;
		this.writer = writer;
		this.threadId = threadId;
	}

	@Override
	public void run() {
		ConsumerIterator consumerIterator = mStream.iterator();
		while (consumerIterator.hasNext()) {
			try {
				byte[] message = (byte[]) consumerIterator.next().message();
				String data = new String(message, Charset.forName("utf-8"));
				boolean success = false;
				int i = 0;
				while (!success && i < 3) {
					try {
						writer.write(data);
						success = true;
					} catch (Exception e) {
					    if(e instanceof RecoverException){
					        log.warn(e, "Recovering");
					    } else {
						    i++;
	                        if (i == 3) {
	                            log.warn(e, "attemp 3 time fail, Discard data = "
	                                    + data);
	                        }
                        }
						try {
							Thread.sleep(1000);
						} catch (Exception e1) {
						}
					}
				}
			} catch (Exception e) {
				log.error(e, "fail to comsume the message from topic");
			}
		}
		log.info("Shutting down Thread: " + threadId + " for writer: "
				+ writer.topic);
	}
}