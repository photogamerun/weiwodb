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

import static java.util.Objects.requireNonNull;

import java.util.List;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;

import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
/**
 * 
 * 
 * @author peter.wei
 *
 */
public class LuceneConnector implements Connector {

	private static final Logger log = Logger.get(LuceneConnector.class);

	private final LifeCycleManager lifeCycleManager;
	private final LuceneMetadata metadata;
	private final LuceneSplitManager splitManager;
	private final LuceneRecordSetProvider recordSetProvider;
	private final List<PropertyMetadata<?>> tableProperties;
	private final ConnectorAccessControl accessControl;

	public LuceneConnector(LifeCycleManager lifeCycleManager,
			LuceneMetadata metadata, LuceneSplitManager splitManager,
			LuceneRecordSetProvider recordSetProvider,
			List<PropertyMetadata<?>> tableProperties,
			ConnectorAccessControl accessControl) {
		this.lifeCycleManager = requireNonNull(lifeCycleManager,
				"lifeCycleManager is null");
		this.metadata = requireNonNull(metadata, "metadata is null");
		this.splitManager = requireNonNull(splitManager,
				"splitManager is null");
		this.tableProperties = requireNonNull(tableProperties,
				"tableProperties is null");
		this.accessControl = requireNonNull(accessControl,
				"accessControl is null");
		this.recordSetProvider = requireNonNull(recordSetProvider,
				"recordSetProvider is null");
	}

	@Override
	public ConnectorTransactionHandle beginTransaction(
			IsolationLevel isolationLevel, boolean readOnly) {
		return LuceneTransactionHandle.INSTANCE;
	}

	@Override
	public ConnectorMetadata getMetadata(
			ConnectorTransactionHandle transactionHandle) {
		return metadata;
	}

	@Override
	public ConnectorSplitManager getSplitManager() {
		return splitManager;
	}

	@Override
	public ConnectorRecordSetProvider getRecordSetProvider() {
		return recordSetProvider;
	}

	@Override
	public List<PropertyMetadata<?>> getTableProperties() {
		return tableProperties;
	}

	@Override
	public final void shutdown() {
		try {
			lifeCycleManager.stop();
		} catch (Exception e) {
			log.error(e, "Error shutting down connector");
		}
	}

	@Override
	public ConnectorAccessControl getAccessControl() {
		return accessControl;
	}

	@Override
	public ConnectorPageSourceProvider getPageSourceProvider() {
		return new LucenePageSourceProvider(recordSetProvider);
	}
}
