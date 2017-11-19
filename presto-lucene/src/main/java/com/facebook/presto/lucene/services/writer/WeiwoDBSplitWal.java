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

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Split Wal Writer 每个{datapath}/{db}/{table}/{partition}/{nodeid}下面只会有一个Split
 * Writer 同时对应一个wal文件,该文件记录当前实时Split Writer写入的数据 当Split Writer正常commit的时候删除wal文件
 * 每个新的Split Writer会创建一个Split Wal Writer
 * 恢复数据Writer不会创建,只会在恢复完成并且索引commit之后删除该Wal
 * 
 * @author folin01.chen
 *
 */
public class WeiwoDBSplitWal {

	private final FileSystem fs;
	private final Path wal;

	private final boolean isOpenForWrite;
	private FSDataOutputStream outStream;
	private FSDataInputStream inStream;

	public WeiwoDBSplitWal(FileSystem fs, Path wal, boolean isOpenForWrite)
			throws IOException {
		this.fs = requireNonNull(fs, "fs is null");
		this.wal = requireNonNull(wal, "editLog is null");
		this.isOpenForWrite = isOpenForWrite;
		fs.mkdirs(wal.getParent());
		if (isOpenForWrite) {
			if (this.fs.exists(this.wal)) {
				fs.rename(this.wal, new Path(this.wal.getParent(),
						this.wal.getName() + "." + System.currentTimeMillis()));
			}
			this.outStream = this.fs.create(this.wal, true, 65535);
		} else {
			this.inStream = this.fs.open(this.wal, 16484);
		}
	}

	public synchronized void write(String odata) throws IOException {
		if (!isOpenForWrite) {
			throw new IOException(
					"Write EditLog, but WeiwoDBEditLog is open for read.");
		}
		byte[] bytes = odata.getBytes(Charset.forName("utf-8"));
		outStream.writeInt(bytes.length);
		outStream.write(bytes);
		// outStream.hflush();
	}

	public synchronized String read() throws IOException {
		if (isOpenForWrite) {
			throw new IOException(
					"Read EditLog, but WeiwoDBEditLog is open for write.");
		}
		try {
			int length = inStream.readInt();
			if (length <= 0) {
				return null;
			}
			byte[] bytes = new byte[length];
			inStream.read(bytes);
			return new String(bytes, Charset.forName("utf-8"));
		} catch (EOFException e) {
			return null;
		}
	}

	public void close() throws IOException {
		if (outStream != null) {
			outStream.close();
		}
		if (inStream != null) {
			inStream.close();
		}
	}

	public void delete() throws IOException {
		fs.delete(wal, true);
	}

	public Path getWalPath() {
		return wal;
	}

	// public static void main(String[] args) throws IOException {
	// UserGroupInformation.setLoginUser(UserGroupInformation.createUserForTesting("hdfs",
	// new String[] { "hdfs" }));
	// FileSystem fs = FileSystem.get(
	// URI.create("hdfs://10.199.221.152:8020/data/weiwodb/default/weiwo/20170113/1/WAL/weiwo.wal"),
	// new Configuration());
	// FSDataInputStream in = fs.open(new
	// Path("/data/weiwodb/default/weiwo/20170113/1/WAL/weiwo.wal"));
	// int length = in.readInt();
	// while (length > 0) {
	// byte[] bytes = new byte[length];
	// in.read(bytes);
	// System.out.println(new String(bytes, Charset.forName("utf-8")));
	// try{
	// length = in.readInt();
	// } catch (EOFException e){
	// length = 0;
	// }
	// }
	// }

}
