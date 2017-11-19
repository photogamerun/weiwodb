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
package com.facebook.presto.lucene.hive.metastore;

import javax.annotation.Nullable;
import javax.inject.Inject;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.facebook.presto.lucene.LuceneConfig;
import com.facebook.presto.lucene.services.security.LuceneMetastoreAuthentication;
import com.facebook.presto.spi.PrestoException;
/**
 * 
 * @author peter.wei
 *
 */
public class LuceneMetastoreClientFactory {

	private String hiveMetaAddress;
	private int hiveMetaPort;
	private int timeOutMs;
	private LuceneMetastoreAuthentication authentication;

	private LuceneMetastoreClientFactory(@Nullable String hiveMetaAddress,
			int hiveMetaPort, int timeOutMs,
			LuceneMetastoreAuthentication authentication) {
		this.hiveMetaAddress = hiveMetaAddress;
		this.hiveMetaPort = hiveMetaPort;
		this.timeOutMs = timeOutMs;
		this.authentication = authentication;
	}

	@Inject
	public LuceneMetastoreClientFactory(LuceneConfig config,
			LuceneMetastoreAuthentication authentication) {
		this(config.getHiveMetaAddress(), config.getHiveMetaPort(),
				config.getTimeOutMs(), authentication);
	}

	public LuceneMetastoreClient create() {
		TTransportException lastException = null;
		try {
			return new LuceneHiveMetastoreClient(
					create(hiveMetaAddress, hiveMetaPort, timeOutMs));
		} catch (TTransportException e) {
			lastException = e;
		}
		throw new PrestoException(LuceneErrorCode.LUCENE_METASTORE_ERROR,
				"Failed connecting to Hive metastore: " + hiveMetaAddress,
				lastException);
	}

	private TTransport create(@Nullable String hiveMetaAddress,
			@Nullable int hiveMetaPort, int timeOutMs)
			throws TTransportException {

		return Transport.create(hiveMetaAddress, hiveMetaPort, null, timeOutMs,
				authentication);
	}
}