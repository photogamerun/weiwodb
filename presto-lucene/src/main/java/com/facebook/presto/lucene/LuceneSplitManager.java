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

import static com.facebook.presto.lucene.Types.checkType;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;

import javax.inject.Inject;

import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.weakref.jmx.internal.guava.base.Throwables;

import com.facebook.presto.lucene.base.IndexInfo;
import com.facebook.presto.lucene.base.IndexInfoWithNodeInfoSimple;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.pushdown.PushDown;
import com.facebook.presto.sql.pair.GroupByPair;
import com.facebook.presto.sql.pair.SimpleFilterPair;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.util.PushDownUtilities;

import io.airlift.http.client.HttpClient;

public class LuceneSplitManager implements ConnectorSplitManager {

	Random random = new Random();
	private final LuceneConfig config;
	private final IndexManagerClient idxManagerclient;
	private RemoteFileSystemClient filesystemClient;
	Class md5 = MD5FileUtils.class;

	@Inject
	public LuceneSplitManager(LuceneConfig config,
			@ForWeiwoManager HttpClient httpClient,
			WeiwoDBHdfsConfiguration wwconf) {
		this.config = requireNonNull(config, "config is null");
		requireNonNull(wwconf, "config is null");
		this.idxManagerclient = new IndexManagerClient(config, httpClient);
		this.filesystemClient = new RemoteFileSystemClient(
				wwconf.getConfiguration());
	}

	@Override
	public ConnectorSplitSource getSplits(
			ConnectorTransactionHandle transaction, ConnectorSession session,
			ConnectorTableLayoutHandle layoutHandle, PushDown pushdown) {
		LuceneTableLayoutHandle layout = checkType(layoutHandle,
				LuceneTableLayoutHandle.class, "layoutHandle");

		String[] partitions = extractParition(pushdown, session.getUser());

		try {
			List<IndexInfoWithNodeInfoSimple> memSplit = idxManagerclient
					.listIndex(layout.getTable().getSchemaName(),
							layout.getTable().getTableName(), partitions);

			List<IndexInfo> hdfsSplit = filesystemClient.getHdfsSplit(
					config.getDataPath(), layout.getTable().getSchemaName(),
					layout.getTable().getTableName(), partitions);
			Iterator<IndexInfoWithNodeInfoSimple> ir = memSplit.iterator();
			while (ir.hasNext()) {
				IndexInfoWithNodeInfoSimple simple = ir.next();
				if (!hdfsSplit.contains(simple.getIndexInfo())) {
					ir.remove();
				}
			}
			List<ConnectorSplit> splits = new ArrayList<>();
			List<IndexInfo> merged = new ArrayList<>();

			if (pushdown.has(GroupByPair.class)) {
				mergeMemSplit(memSplit, merged, splits, layout, 1);
			} else {
				mergeMemSplit(memSplit, merged, splits, layout,
						config.getMultiReaderMaxNum());
			}

			for (IndexInfo info : hdfsSplit) {
				if (!merged.contains(info)) {
					List<HostAddress> address = new ArrayList<>();
					List<IndexInfo> indexs = new ArrayList<>();
					indexs.add(info);
					splits.add(new LuceneSplit(
							layout.getTable().getConnectorId(),
							layout.getTable().getCatalogName(), indexs, address,
							layout.getTupleDomain(), new Boolean(false)));
				}
			}

			List<IndexInfoWithNodeInfoSimple> memWriter = idxManagerclient
					.listIndexWriter(layout.getTable().getSchemaName(),
							layout.getTable().getTableName(), partitions);

			for (IndexInfoWithNodeInfoSimple writer : memWriter) {
				List<HostAddress> address = Arrays
						.asList(new HostAddress[]{
								writer.getNodes()
										.get(random.nextInt(
												writer.getNodes().size()))
										.getAddress()});
				List<IndexInfo> indexs = new ArrayList<>();
				indexs.add(writer.getIndexInfo());
				splits.add(new LuceneSplit(layout.getTable().getConnectorId(),
						layout.getTable().getCatalogName(), indexs, address,
						layout.getTupleDomain(), new Boolean(true)));
			}
			return new FixedSplitSource(splits);
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}

	public void mergeMemSplit(List<IndexInfoWithNodeInfoSimple> memSplit,
			List<IndexInfo> merged, List<ConnectorSplit> splits,
			LuceneTableLayoutHandle layout, int mutilReader) {
		if (memSplit != null) {
			Map<MergeKey, List<IndexInfoWithNodeInfoSimple>> ms = new HashMap<>();
			memSplit.forEach(split -> {
				if (split.getNodes() != null && split.getNodes().size() >= 1) {
					HostAddress ha = split.getNodes()
							.get(random.nextInt(split.getNodes().size()))
							.getAddress();
					String partition = split.getIndexInfo().getPartition();
					MergeKey key = new MergeKey(ha, partition);
					List<IndexInfoWithNodeInfoSimple> list = ms.get(key);
					if (list == null) {
						list = new ArrayList<>();
						list.add(split);
						ms.put(key, list);
					} else {
						list.add(split);
					}
				}
			});
			Iterator<Entry<MergeKey, List<IndexInfoWithNodeInfoSimple>>> ir = ms
					.entrySet().iterator();
			while (ir.hasNext()) {
				Entry<MergeKey, List<IndexInfoWithNodeInfoSimple>> value = ir
						.next();
				int mergeNum = getNumAfterMerge(value.getValue(), mutilReader);
				int size = value.getValue().size();
				if (size > 0) {
					for (int i = 0; i < mergeNum; i++) {
						List<IndexInfo> indexs = new ArrayList<>();
						for (int j = 0; j < mutilReader; j++) {
							int cursor = i * mutilReader + j;
							if (cursor >= size) {
								break;
							}
							indexs.add(value.getValue().get(cursor)
									.getIndexInfo());
						}
						merged.addAll(indexs);
						LuceneSplit split = new LuceneSplit(
								layout.getTable().getConnectorId(),
								layout.getTable().getCatalogName(), indexs,
								Arrays.asList(new HostAddress[]{
										value.getKey().getHostAddress()}),
								layout.getTupleDomain(), new Boolean(false));
						splits.add(split);
					}
				}
			}
		}
	}

	public int getNumAfterMerge(List<IndexInfoWithNodeInfoSimple> values,
			int splitNum) {
		int size = values.size();
		int num = 0;
		if ((size % splitNum) == 0) {
			num = size / splitNum;
		} else {
			num = size / splitNum + 1;
		}
		return num;
	}

	static class MergeKey {
		private HostAddress hostAddress;
		private String partition;

		public MergeKey(HostAddress hostAddress, String partition) {
			this.hostAddress = hostAddress;
			this.partition = partition;
		}

		public HostAddress getHostAddress() {
			return hostAddress;
		}

		public void setHostAddress(HostAddress hostAddress) {
			this.hostAddress = hostAddress;
		}

		public String getPartition() {
			return partition;
		}

		public void setPartition(String partition) {
			this.partition = partition;
		}

		@Override
		public int hashCode() {
			return Objects.hash(hostAddress, partition);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null || getClass() != obj.getClass()) {
				return false;
			}
			final MergeKey other = (MergeKey) obj;
			if (other.getHostAddress() == null
					|| other.getPartition() == null) {
				return false;
			}
			return (other.getHostAddress().equals(this.getHostAddress())
					&& other.getPartition().equals(this.getPartition()));
		}

	}

	// 从where 条件中抓取 vpartiton 信息。vpartition 不抓取 > < 和非字符串类型的值
	private String[] extractParition(PushDown pushdown, String user) {
		Expression filter = pushdown.getValue(SimpleFilterPair.class);
		filter = filter == null ? BooleanLiteral.TRUE_LITERAL : filter;
		String[] partitions = PushDownUtilities.extractPartition(filter);
		if (partitions.length == 0) {
			throw new RuntimeException(
					"fail to find vpartition from where clause: reject query "
							+ filter + " from user " + user);
		}
		return partitions;
	}
}