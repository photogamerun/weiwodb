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

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;
import com.facebook.presto.lucene.services.writer.exception.RecoverException;

import io.airlift.log.Logger;

/**
 * DataSource 所有数据源的实现必须继承WeiwoDBDataWriter,使用writerData写入数据,数据类型为json字符串
 * 
 * @author folin01.chen
 *
 */
public abstract class WeiwoDBDataWriter {

	private static final Logger log = Logger.get(WeiwoDBDataWriter.class);

	private WeiwoDBSplitWriterManager writerManager;
	private String nodeId;
	private String name;
	private int number;
	private String pathId;
	Properties properties;

	public WeiwoDBDataWriter() {
	}

	public WeiwoDBDataWriter(String nodeId, String name, int number) {
		this.nodeId = nodeId;
		this.name = name;
		this.number = number;
		this.pathId = WriterIdUtil.genarate(nodeId, number);
	}

	public abstract void init() throws Exception;

	public abstract void close() throws Exception;

	public void setSplitWriterManager(WeiwoDBSplitWriterManager writerManager) {
		this.writerManager = writerManager;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setPathId(String pathId) {
		this.pathId = pathId;
	}

	public String getNodeId() {
		return this.nodeId;
	}

	public String getName() {
		return this.name;
	}

	public void setNumber(int number) {
		this.number = number;
	}

	public int getNumber() {
		return this.number;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public void write(String message)
			throws IllegalArgumentException, IOException, RecoverException {
		JSONObject json = JSON.parseObject(message);
		String db = json.getString(WeiwoDBConfigureKeys.JSON_DB_KEY);
		if (db == null) {
			log.error("missing db info message " + message);
		}
		String table = json.getString(WeiwoDBConfigureKeys.JSON_TABLE_KEY);
		String partition = json
				.getString(WeiwoDBConfigureKeys.JSON_PARTITION_KEY);
		JSONArray datas = json
				.getJSONArray(WeiwoDBConfigureKeys.JSON_DATAS_KEY);
		WeiwoDBSplitNormalWriter writer = writerManager
				.getNormalSplitWriterByKey(db, table, partition, pathId);
		if (writer == null) {
			writer = writerManager.createNormalSplitWriterByKey(db, table,
					partition, pathId);
		}
		if (writer != null) {
			writer.writeWal(message + "\n");
			writeIndex(message, db, table, partition, datas, writer);
		} else {
			throw new IOException("WeiwoDBSplitNormalWriter is null");
		}
	}

	private synchronized void writeIndex(String message, String db,
			String table, String partition, JSONArray datas,
			WeiwoDBSplitNormalWriter writer) throws IOException {
		writer.write(datas, message.length());
		boolean need = writer.checkNeedFlushAndClose();
		if (need) {
			flushAndClose(writer, db, table, partition, pathId);
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
				log.error(e, "flushAndClose deleteZK error.");
			}
			try {
				writer.getWal().close();
				writer.getWal().delete();
			} catch (Exception e) {
				log.error(e, "flushAndClose wal delete error.");
			}
			try {
				writer.unRegisterWriter();
			} catch (Exception e) {
				log.error(e, "flushAndClose unRegisterWriter error.");
			}
		} catch (Exception e) {
			log.error(e, "flushAndClose error.");
			try {
				writer.deleteZK();
			} catch (Exception ex) {
				log.error(ex, "flushAndClose deleteZK error.");
			}
			try {
				writer.deleteHdfs();
			} catch (Exception ex) {
				log.error(ex, "flushAndClose wal delete error.");
			}
			try {
				writer.unRegisterWriter();
			} catch (Exception ex) {
				log.error(e, "flushAndClose unRegisterWriter error.");
			}
			try {
				writer.close();
			} catch (Exception ex) {
				log.error(e, "flushAndClose close error.");
			}
			try {
				writer.closeMem();
			} catch (Exception ex) {
				log.error(e, "flushAndClose closeMem error.");
			}
			writerManager.failed(db, table, partition, id, 3);
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(nodeId, name, number);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		final WeiwoDBDataWriter other = (WeiwoDBDataWriter) obj;
		if (this.name == null || this.nodeId == null) {
			return false;
		}
		return this.nodeId.equals(other.getNodeId())
				&& this.name.equals(other.getName())
				&& this.number == other.getNumber();
	}

}
