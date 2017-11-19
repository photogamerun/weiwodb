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
package com.facebook.presto.lucene.services.writer;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.com.esotericsoftware.minlog.Log;
import org.weakref.jmx.internal.guava.base.Joiner;

import com.facebook.presto.lucene.ForWeiwoManager;
import com.facebook.presto.lucene.LuceneConfig;
import com.facebook.presto.lucene.WeiwoDBHdfsConfiguration;
import com.facebook.presto.lucene.ZkClientFactory;
import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;
import com.facebook.presto.lucene.services.writer.exception.RecoverException;
import com.facebook.presto.lucene.services.writer.util.WalUtils;
import com.facebook.presto.lucene.util.WriterTree;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Recover;
import com.facebook.presto.spi.RecoverAdapterInterface;

import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;

/**
 * Split Writer Manager 管理此节点的所有Split
 * Writer,所有数据源的WeiwoDBDataWriter共用对象,即一个表可以接收来自不同数据源的数据
 * 
 * @author folin01.chen
 *
 */
public class WeiwoDBSplitWriterManager implements Recover {

	private static final Logger log = Logger
			.get(WeiwoDBSplitWriterManager.class);

	public static final String KEY_SEP = "_-_";

	private WriterTree<String, WeiwoDBSplitNormalWriter> wcache;
	private WriterTree<String, WeiwoDBSplitRecoverWriter> rwcache;
	private Set<WriterKey> normalWriter;
	private Set<String> needRecoverSplitKes;

	NodeManager nodeManager;
	ReentrantReadWriteLock lock;
	WriteLock writeLock;
	ReadLock readLock;
	WeiwoDBSplitWriterFactory createFactory;
	LuceneConfig config;
	ZkClientFactory zkClientFactory;
	WeiwoDBHdfsConfiguration wconfig;
	HttpClient httpClient;
	Configuration conf;
	FileSystem fs;
	ScheduledExecutorService closeService;
	final WriterChecker checker;
	final Thread checkerThread;
	final ZookeeperLuceneFieldFetcher zkFieldFetcher;
	final SplitCompactService compactService;

	@Inject
	public WeiwoDBSplitWriterManager(LuceneConfig config,
			ZkClientFactory zkClientFactory, WeiwoDBHdfsConfiguration wconfig,
			@ForWeiwoManager HttpClient httpClient,
			ZookeeperLuceneFieldFetcher zkFieldFetcher, NodeManager nodeManager,
			RecoverAdapterInterface recoverAdapter,
			SplitCompactService compactService) throws IOException {
		this.wcache = new WriterTree<String, WeiwoDBSplitNormalWriter>(
				"normal");
		this.rwcache = new WriterTree<String, WeiwoDBSplitRecoverWriter>(
				"recover");
		this.normalWriter = new HashSet<>();
		this.needRecoverSplitKes = new HashSet<>();
		this.lock = new ReentrantReadWriteLock(true);
		this.writeLock = this.lock.writeLock();
		this.readLock = this.lock.readLock();
		this.createFactory = new WeiwoDBSplitWriterFactory();
		this.config = requireNonNull(config, "config is null");
		this.zkClientFactory = requireNonNull(zkClientFactory,
				"zkClientFactory is null");
		this.wconfig = requireNonNull(wconfig, "wconfig is null");
		this.httpClient = requireNonNull(httpClient, "httpClient is null");
		this.zkFieldFetcher = requireNonNull(zkFieldFetcher,
				"zkFieldFetcher is null");
		this.compactService = requireNonNull(compactService,
				"compactService is null");
		this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
		this.closeService = Executors.newScheduledThreadPool(4);
		this.conf = wconfig.getConfiguration();
		this.fs = FileSystem.get(conf);
		this.checker = new WriterChecker(this);
		this.checkerThread = new Thread(checker);
		checkerThread.setName("Writer checker");
		checkerThread.setDaemon(true);
		checkerThread.start();
		recoverAdapter.setRecoverInstance(this);
	}

	public void checkRecover(String key) throws RecoverException {
		if (needRecoverSplitKes.contains(key)) {
			throw new RecoverException(
					"Split wait to Recove or split is recovering. Waiting recover finished. key = "
							+ key);
		}
	}

	public WeiwoDBSplitNormalWriter getNormalSplitWriterByKey(String db,
			String table, String partition, String id) {
		readLock.lock();
		try {
			return wcache.get(db, table, partition, id);
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public boolean startRecover(Map<String, Object> properties) {
		requireNonNull(properties, "recover properties is null");
		String db = requireNonNull(properties.get("db"), "db is null")
				.toString();
		String table = requireNonNull(properties.get("table"), "table is null")
				.toString();
		String partition = requireNonNull(properties.get("partition"),
				"partition is null").toString();
		String id = requireNonNull(properties.get("id"), "id isnull")
				.toString();
		String indexName = requireNonNull(properties.get("indexName"),
				"indexName is null").toString();
		String dataPath = requireNonNull(properties.get("dataPath"),
				"dataPath is null").toString();
		Map<String, String> luceneFields = (Map<String, String>) requireNonNull(
				properties.get("luceneFields"), "luceneFields is null");
		WeiwoDBSplitWal walReader = null;
		String key = genarateKey(db, table, partition, id);
		try {
			checkRecover(key);
		} catch (RecoverException e) {
			log.error(e, key + ": already in recovering.");
			return false;
		}
		try {
			walReader = new WeiwoDBSplitWal(fs, new Path(WalUtils
					.genarateWalPath(dataPath, db, table, partition, id)),
					false);
		} catch (IllegalArgumentException | IOException e) {
			log.error(e, "wal reader init.");
			if (walReader != null) {
				try {
					walReader.close();
				} catch (IOException e1) {
					log.error(e1, "close wal reader.");
				}
			}
			return false;
		}
		RemoteRequestRecoverCallBack callBack = new RemoteRequestRecoverCallBack(
				httpClient, this, db, table, partition, id, config);
		writeLock.lock();
		try {
			needRecoverSplitKes.add(key);
			WeiwoDBSplitRecoverWriter writer = createFactory.createRecover(
					config, db, table, partition, id, zkClientFactory,
					luceneFields, indexName, conf, fs, httpClient, walReader,
					nodeManager.getCurrentNode(), callBack);
			if (writer != null) {
				rwcache.add(writer, db, table, partition, id);
				if (isCurrentNode(id)) {
					compactService.addCompactSplits(db, table, partition, id);
				}
				return true;
			} else {
				Log.error("Create Recover return null. retry remain times.");
				needRecoverSplitKes.remove(key);
				return false;
			}
		} finally {
			writeLock.unlock();
		}
	}

	public WeiwoDBSplitNormalWriter createNormalSplitWriterByKey(String db,
			String table, String partition, String id)
			throws IllegalArgumentException, IOException, RecoverException {
		writeLock.lock();
		try {
			checkRecover(genarateKey(db, table, partition, id));
			WeiwoDBSplitNormalWriter writer = wcache.get(db, table, partition,
					id);
			if (writer == null) {
				Map<String, String> lfs = zkFieldFetcher.getLuceneFields(db,
						table);
				if (lfs == null) {
					return null;
				}
				writer = createFactory.createNormal(config, db, table,
						partition, id, zkClientFactory, lfs, conf, fs,
						httpClient,
						new WeiwoDBSplitWal(fs,
								new Path(WalUtils.genarateWalPath(
										config.getDataPath(), db, table,
										partition, id)),
								true),
						nodeManager.getCurrentNode(), closeService);
				if (writer != null) {
					wcache.add(writer, db, table, partition, id);
					normalWriter.add(new WriterKey(db, table, partition, id));
					if (isCurrentNode(id)) {
						compactService.addCompactSplits(db, table, partition,
								id);
					}
				}
			}
			return writer;
		} finally {
			writeLock.unlock();
		}
	}

	private boolean isCurrentNode(String id) {
		return config.getNodeId().equals(WriterIdUtil.split(id)[0]);
	}

	public WeiwoDBSplitWriter getSplitWriterByKey(String db, String table,
			String partition, String id) {
		readLock.lock();
		try {
			WeiwoDBSplitNormalWriter writer = wcache.get(db, table, partition,
					id);
			if (writer != null) {
				return writer;
			} else {
				return rwcache.get(db, table, partition, id);
			}
		} finally {
			readLock.unlock();
		}
	}

	public WeiwoDBSplitRecoverWriter getRecoverSplitWriterByKey(String db,
			String table, String partition, String id) {
		readLock.lock();
		try {
			return rwcache.get(db, table, partition, id);
		} finally {
			readLock.unlock();
		}
	}

	public void complete(String db, String table, String partition, String id) {
		writeLock.lock();
		try {
			WeiwoDBSplitNormalWriter writer = wcache.remove(db, table,
					partition, id);
			normalWriter.remove(new WriterKey(db, table, partition, id));
			closeService.schedule(new WeiwoDBWriterCloseTask(writer,
					WeiwoDBConfigureKeys.CLOSE_DELAY_TIME, closeService), 0,
					TimeUnit.MILLISECONDS);
			if (isCurrentNode(id)) {
				compactService.addCompactSplits(db, table, partition, id);
			}
		} finally {
			writeLock.unlock();
		}
	}

	public void failed(String db, String table, String partition, String id,
			int num) {
		String key = genarateKey(db, table, partition, id);
		Log.info("Commit failed . split = " + key);
		WeiwoDBSplitNormalWriter writer = null;
		writeLock.lock();
		try {
			writer = wcache.get(db, table, partition, id);
			try {
				writer.getWal().close();
			} catch (IOException e) {
				log.warn(e, "Close wal error.");
			}
			needRecoverSplitKes.add(key);
			WeiwoDBSplitWal walReader = new WeiwoDBSplitWal(fs,
					writer.getWal().getWalPath(), false);
			WeiwoDBSplitRecoverWriter nwriter = createFactory.createRecover(
					config, writer.schema, writer.table, writer.partition,
					writer.id, zkClientFactory, writer.luceneFields,
					writer.indexName, conf, fs, httpClient, walReader,
					nodeManager.getCurrentNode(), new SplitRecoverCallBack(this,
							db, table, partition, id, closeService));
			if (nwriter != null) {
				wcache.remove(db, table, partition, id);
				normalWriter.remove(new WriterKey(db, table, partition, id));
				rwcache.add(nwriter, db, table, partition, id);
			} else {
				Log.info("Create Recover return null. retry remain times = "
						+ num);
				if (num > 0) {
					failed(db, table, partition, id, num - 1);
				} else {
					wcache.remove(db, table, partition, id);
					normalWriter
							.remove(new WriterKey(db, table, partition, id));
					needRecoverSplitKes
							.remove(genarateKey(db, table, partition, id));
					Log.warn("Failed to recover . rename wal from ["
							+ writer.getWal().getWalPath().toString() + "] to ["
							+ writer.getWal().getWalPath().toString() + "."
							+ writer.indexName + "]");
					try {
						fs.rename(writer.getWal().getWalPath(), writer.getWal()
								.getWalPath().suffix("." + writer.indexName));
					} catch (IOException e) {
						Log.error("Failed to rename wal file.", e);
					}
				}
			}
		} catch (IOException e) {
			log.error(e,
					"Create wal Reader error. retry remain times = " + num);
			if (num > 0) {
				failed(db, table, partition, id, num - 1);
			} else {
				wcache.remove(db, table, partition, id);
				normalWriter.remove(new WriterKey(db, table, partition, id));
				needRecoverSplitKes
						.remove(genarateKey(db, table, partition, id));
				Log.warn("Failed to recover . rename wal from ["
						+ writer.getWal().getWalPath().toString() + "] to ["
						+ writer.getWal().getWalPath().toString() + "."
						+ writer.indexName + "]");
				try {
					fs.rename(writer.getWal().getWalPath(), writer.getWal()
							.getWalPath().suffix("." + writer.indexName));
				} catch (IOException ex) {
					Log.error("Failed to rename wal file.", ex);
				}
			}
		} finally {
			writeLock.unlock();
		}
	}

	public void completeRecover(String db, String table, String partition,
			String id) {
		writeLock.lock();
		try {
			WeiwoDBSplitRecoverWriter writer = rwcache.remove(db, table,
					partition, id);
			needRecoverSplitKes.remove(genarateKey(db, table, partition, id));
			closeService.schedule(new WeiwoDBWriterCloseTask(writer,
					WeiwoDBConfigureKeys.CLOSE_DELAY_TIME, closeService), 0,
					TimeUnit.MILLISECONDS);
			if (isCurrentNode(id)) {
				compactService.addCompactSplits(db, table, partition, id);
			}
		} finally {
			writeLock.unlock();
		}
	}

	public void retryRecover(String db, String table, String partition,
			String id, int num) {
		Log.info("Retry recover after 2s . split = "
				+ genarateKey(db, table, partition, id));
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
		}
		writeLock.lock();
		try {
			WeiwoDBSplitRecoverWriter writer = rwcache.get(db, table, partition,
					id);
			WeiwoDBSplitRecoverWriter nwriter = createFactory.createRecover(
					config, writer.db, writer.table, writer.partition,
					writer.id, zkClientFactory, writer.luceneFields,
					writer.indexName, conf, fs, httpClient, writer.walReader,
					nodeManager.getCurrentNode(), new SplitRecoverCallBack(this,
							db, table, partition, id, closeService));
			if (nwriter != null) {
				rwcache.add(nwriter, db, table, partition, id);
			} else {
				Log.info("Create Recover return null. retry remain times = "
						+ num);
				if (num > 0) {
					retryRecover(db, table, partition, id, num - 1);
				} else {
					rwcache.remove(db, table, partition, id);
					needRecoverSplitKes
							.remove(genarateKey(db, table, partition, id));
					Log.warn("Failed to recover . rename wal from ["
							+ writer.walReader.getWalPath().toString()
							+ "] to ["
							+ writer.walReader.getWalPath().toString() + "."
							+ writer.indexName + "]");
					try {
						fs.rename(writer.walReader.getWalPath(),
								writer.walReader.getWalPath()
										.suffix("." + writer.indexName));
					} catch (IOException e) {
						Log.error("Failed to rename wal file.", e);
					}
				}
			}
		} finally {
			writeLock.unlock();
		}
	}

	public static String genarateKey(String db, String table, String partition,
			String id) {
		requireNonNull(db, "db is null");
		requireNonNull(table, "table is null");
		requireNonNull(partition, "partition is null");
		requireNonNull(id, "id is null");
		return Joiner.on(KEY_SEP).join(db, table, partition, id);
	}

	static class WriterKey {
		String db;
		String table;
		String partition;
		String id;

		public WriterKey(String db, String table, String partition, String id) {
			this.db = db;
			this.table = table;
			this.partition = partition;
			this.id = id;
		}

		public String getDb() {
			return db;
		}

		public void setDb(String db) {
			this.db = db;
		}

		public String getTable() {
			return table;
		}

		public void setTable(String table) {
			this.table = table;
		}

		public String getPartition() {
			return partition;
		}

		public void setPartition(String partition) {
			this.partition = partition;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		@Override
		public int hashCode() {
			return Objects.hash(db, table, partition, id);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null || getClass() != obj.getClass()) {
				return false;
			}
			final WriterKey other = (WriterKey) obj;
			if (other.db == null || other.table == null
					|| other.partition == null || other.id == null
					|| this.db == null || this.table == null
					|| this.partition == null || this.id == null) {
				return false;
			}
			return (other.db.equals(this.db) && other.table.equals(this.table)
					&& other.partition.equals(this.partition)
					&& other.id.equals(this.id));
		}
	}

	class WriterChecker implements Runnable {

		final WeiwoDBSplitWriterManager writerManager;

		public WriterChecker(WeiwoDBSplitWriterManager writerManager) {
			this.writerManager = writerManager;
		}

		@Override
		public void run() {
			List<WriterKey> list = new ArrayList<>();
			while (true) {
				try {
					Thread.sleep(60l * 1000l);
				} catch (InterruptedException e) {
				}
				list.clear();
				list.addAll(normalWriter);
				for (WriterKey key : list) {
					WeiwoDBSplitNormalWriter writer = writerManager
							.getNormalSplitWriterByKey(key.db, key.table,
									key.partition, key.id);
					if (writer.checkNeedFlushAndClose()) {
						flushAndClose(writer, key.db, key.table, key.partition,
								key.id);
					}
				}
			}
		}

		private void flushAndClose(WeiwoDBSplitNormalWriter writer, String db,
				String table, String partition, String id) {
			try {
				log.info("Flush and close writer. " + WeiwoDBSplitWriterManager
						.genarateKey(db, table, partition, id));
				writer.commit();
				writer.addSplitInfo();
				writerManager.complete(db, table, partition, id);
				try {
					writer.deleteZK();
				} catch (Exception e) {
					log.error(e, "WriterChecker deleteZK error.");
				}
				try {
					writer.getWal().close();
					writer.getWal().delete();
				} catch (Exception e) {
					log.error(e, "WriterChecker wal delete error.");
				}
				try {
					writer.unRegisterWriter();
				} catch (Exception e) {
					log.error(e, "WriterChecker unRegisterWriter error.");
				}
			} catch (Exception e) {
				log.error(e, "flushAndClose error.");
				try {
					writer.deleteZK();
				} catch (Exception ex) {
					log.error(ex, "WriterChecker deleteZK error.");
				}
				try {
					writer.deleteHdfs();
				} catch (Exception ex) {
					log.error(ex, "WriterChecker wal delete error.");
				}
				try {
					writer.unRegisterWriter();
				} catch (Exception ex) {
					log.error(e, "WriterChecker unRegisterWriter error.");
				}
				try {
					writer.close();
				} catch (Exception ex) {
					log.error(e, "WriterChecker close error.");
				}
				try {
					writer.closeMem();
				} catch (Exception ex) {
					log.error(e, "WriterChecker closeMem error.");
				}
				writerManager.failed(db, table, partition, id, 3);
			}
		}

	}

}
