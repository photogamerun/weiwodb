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
package com.facebook.presto.lucene.index;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.com.esotericsoftware.minlog.Log;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;

import com.facebook.presto.lucene.ForWeiwoManager;
import com.facebook.presto.lucene.IndexManagerClient;
import com.facebook.presto.lucene.LuceneConfig;
import com.facebook.presto.lucene.LuceneSplit;
import com.facebook.presto.lucene.WeiwoDBHdfsConfiguration;
import com.facebook.presto.lucene.base.IndexInfo;
import com.facebook.presto.lucene.base.IndexInfoWithNodeInfoSimple;
import com.facebook.presto.lucene.base.WeiwoDBMemoryInfo;
import com.facebook.presto.lucene.base.WeiwoDBNodeInfo;
import com.facebook.presto.lucene.base.util.IndexSplitKeyUtil;
import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;
import com.facebook.presto.lucene.hdfs.HdfsDirectory;
import com.facebook.presto.lucene.hdfs.UpdateTaskData;
import com.facebook.presto.lucene.util.LuceneUtils;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.googlecode.concurrentlinkedhashmap.CacheConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.CacheWeigher;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;

import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;

public class CacheIndexManager {

	private static final Logger log = Logger.get(CacheIndexManager.class);

	private final ExecutorService threadPool;
	private final LuceneConfig config;
	private final WeiwoDBHdfsConfiguration wwconf;
	private long totalMem;
	private long indexTotalMem;

	private IndexManagerClient idxManagerClient;

	public static int OPEN_TIMEOUT = 30;
	public static int CACHE_CAPACITY_RATIO = 40; // percent in 100
	public static int THREAD_NUM = 10;
	public static long LOADING_TIME_OUT = 300l * 1000l;
	public final CacheConcurrentLinkedHashMap<String, WeiwoIndexReader> cache;
	private final ArrayList<String> loading = new ArrayList<>();
	private final HostAddress hostAddress;
	final NodeManager nodeManager;
	ScheduledExecutorService closeService;
	public LinkedBlockingQueue<UpdateTaskData> updateQueue;
	Thread updateThread;

	@Inject
	public CacheIndexManager(LuceneConfig config,
			@ForWeiwoManager HttpClient httpClient,
			WeiwoDBHdfsConfiguration wwconf, NodeManager nodeManager) {
		this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
		this.hostAddress = nodeManager.getCurrentNode().getHostAndPort();
		this.idxManagerClient = new IndexManagerClient(config, httpClient);
		this.wwconf = requireNonNull(wwconf, "wwconf is null");
		this.config = requireNonNull(config, "config is null");
		this.threadPool = Executors.newFixedThreadPool(
				Runtime.getRuntime().availableProcessors() * 2);
		this.totalMem = Runtime.getRuntime().maxMemory();
		this.indexTotalMem = (totalMem * CACHE_CAPACITY_RATIO) / 100;
		this.cache = new CacheConcurrentLinkedHashMap.Builder<String, WeiwoIndexReader>()
				.maximumWeightedCapacity(indexTotalMem)
				.weigher(new SizeWeighter())
				.listener(new LuceneEvictionListener()).build();
		this.closeService = Executors.newScheduledThreadPool(4);
		this.updateQueue = new LinkedBlockingQueue<>();
		this.updateThread = new Thread(new UpdateCacheWeightService());
		this.updateThread.setName("Cache weighted size update thread");
		this.updateThread.setDaemon(true);
		this.updateThread.start();
	}

	public List<WeiwoIndexReader> getIndexReader(LuceneSplit split)
			throws InterruptedException, ExecutionException, TimeoutException {
		List<WeiwoIndexReader> result = new ArrayList<>();
		List<IndexInfo> remaining = new ArrayList<>();
		try {
			for (IndexInfo index : split.getIndexs()) {
				String key = IndexSplitKeyUtil.getIndexInfo(index.getSchema(),
						index.getTable(), index.getPartition(), index.getId(),
						index.getIndexName());
				WeiwoIndexReader value = cache.get(key);
				if (value != null) {
					// avoid reader close
					value.getReader();
					result.add(value);
				} else {
					remaining.add(index);
				}
			}
			if (remaining.size() == 0) {
				return result;
			}
			List<Future<WeiwoIndexReader>> futures = new ArrayList<>();
			for (IndexInfo index : remaining) {
				futures.add(loadingIndex(index));
			}
			long time = System.currentTimeMillis();
			while (true) {
				if (System.currentTimeMillis() - time > LOADING_TIME_OUT) {
					throw new TimeoutException("Loading index timeout : "
							+ LOADING_TIME_OUT + " ms");
				}
				Iterator<Future<WeiwoIndexReader>> ir = futures.iterator();
				while (ir.hasNext()) {
					Future<WeiwoIndexReader> future = ir.next();
					if (future.isDone()) {
						WeiwoIndexReader r = future.get();
						// avoid reader close
						if (r != null) {
							result.add(r);
						}
						ir.remove();
					}
				}
				if (futures.size() == 0) {
					break;
				}
				Thread.sleep(3000);
			}
			return result;
		} catch (Throwable e) {
			for (WeiwoIndexReader reader : result) {
				reader.decRef();
			}
			throw e;
		}
	}

	private Future<WeiwoIndexReader> loadingIndex(IndexInfo index) {
		Future<WeiwoIndexReader> future = threadPool
				.submit(new Callable<WeiwoIndexReader>() {
					@Override
					public WeiwoIndexReader call()
							throws IOException, InterruptedException {
						String key = IndexSplitKeyUtil.getIndexInfo(
								index.getSchema(), index.getTable(),
								index.getPartition(), index.getId(),
								index.getIndexName());
						synchronized (loading) {
							while (loading.contains(key)) {
								loading.wait(100);
							}
							loading.add(key);
						}
						WeiwoIndexReader wir = null;
						HdfsDirectory directory = null;
						IndexReader reader = null;
						try {
							WeiwoIndexReader value = cache.get(key);
							if (value != null) {
								return value;
							}
							Configuration conf = wwconf.getConfiguration();
							Path path = new Path(new Path(config.getDataPath()),
									index.getSchema() + LuceneUtils.FILE_SEP
											+ index.getTable()
											+ LuceneUtils.FILE_SEP
											+ index.getPartition()
											+ LuceneUtils.FILE_SEP
											+ index.getId()
											+ LuceneUtils.FILE_SEP
											+ index.getIndexName());
							directory = new HdfsDirectory(key, path, conf,
									FileSystem.get(conf), true, updateQueue);
							reader = DirectoryReader.open(directory);
							IndexInfo indexInfo = new IndexInfo(
									index.getSchema(), index.getTable(),
									index.getPartition(), index.getId(),
									index.getIndexName());
							IndexInfoWithNodeInfoSimple simple = createIndexInfo(
									indexInfo);
							if (idxManagerClient.register(simple)) {
								wir = new WeiwoIndexReader(reader,
										directory.totalBytes(),
										directory.ramBytesUsed(), directory,
										path.toString());
								wir.getReader();
								cache.put(key, wir);
								directory.startUpdate();
								log.info("Current cache info. indexTotalMem = "
										+ indexTotalMem + " . weightedSize = "
										+ cache.weightedSize());
								return wir;
							} else {
								log.error("Register error.");
								if (reader != null) {
									reader.close();
								}
								if (directory != null) {
									directory.close();
								}
								return null;
							}
						} catch (IOException e) {
							log.error(e, "Open index error.");
							if (reader != null) {
								reader.close();
							}
							if (directory != null) {
								directory.close();
							}
							throw e;
						} finally {
							synchronized (loading) {
								loading.remove(key);
							}
						}
					}
				});
		return future;
	}

	private IndexInfoWithNodeInfoSimple createIndexInfo(IndexInfo index) {
		WeiwoDBNodeInfo node = new WeiwoDBNodeInfo(
				config.getNodeId(), new WeiwoDBMemoryInfo(totalMem,
						indexTotalMem, cache.weightedSize()),
				hostAddress, createUri());
		requireNonNull(index, "index is null");
		return new IndexInfoWithNodeInfoSimple(index,
				Collections.singletonList(node));
	}

	private String createUri() {
		return this.nodeManager.getCurrentNode().getHttpUri().toString();
	}

	class LuceneEvictionListener
			implements
				EvictionListener<String, WeiwoIndexReader> {

		@Override
		public void onEviction(String key, WeiwoIndexReader value) {
			log.info("Close RAMDirect for:" + key);
			IndexInfo index = IndexSplitKeyUtil.getIndexInfoFromKey(key);
			value.getDirectory().stopUpdate();
			idxManagerClient.close(createIndexInfo(index));
			closeService.schedule(new WeiwoIndexReaderCloseTask(value,
					WeiwoDBConfigureKeys.CLOSE_DELAY_TIME, closeService), 0,
					TimeUnit.MILLISECONDS);
			log.info("Current cache info. indexTotalMem = " + indexTotalMem
					+ " . weightedSize = " + cache.weightedSize());
		}

	}

	static class SizeWeighter implements CacheWeigher<WeiwoIndexReader> {

		@Override
		public long weightOf(WeiwoIndexReader value) {
			if (value == null) {
				return 0;
			} else {
				return value.getInitUsedBytes();
			}
		}

	}

	class UpdateCacheWeightService implements Runnable {

		@Override
		public void run() {
			while (true) {
				try {
					UpdateTaskData task = updateQueue.take();
					cache.update(task.getKey(), task.getWeight());
					log.info("Current cache info. indexTotalMem = "
							+ indexTotalMem + " . weightedSize = "
							+ cache.weightedSize());
				} catch (Throwable e) {
					Log.error("error in UpdateCacheWeightService", e);
				}
			}
		}

	}
}
