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
package com.facebook.presto.lucene.hdfs.blockcache;

import java.io.EOFException;
import java.io.IOException;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import com.facebook.presto.lucene.hdfs.HdfsDirectory;

/**
 * @lucene.experimental
 */
public abstract class CustomBufferedIndexInput extends IndexInput {

	public static final int BUFFER_SIZE = HdfsDirectory.BUFFER_SIZE;

	boolean clone = false;

	private int bufferSize = BUFFER_SIZE;

	protected Object[] buffer;

	private int bufferIndex = 0;
	private int bufferPosition = 0; // next byte to read

	private Store store;

	@Override
	public byte readByte() throws IOException {
		if ((bufferIndex * bufferSize + bufferPosition + 1) > length()) {
			throw new EOFException("read past EOF: " + this);
		}
		if (bufferPosition >= bufferSize) {
			bufferIndex++;
			bufferPosition = bufferPosition % bufferSize;
		}
		if (buffer[bufferIndex] == null) {
			refill(bufferSize - bufferPosition);
		}
		return ((byte[]) buffer[bufferIndex])[bufferPosition++];
	}

	public CustomBufferedIndexInput(String resourceDesc) {
		this(resourceDesc, BUFFER_SIZE);
	}

	public CustomBufferedIndexInput(String resourceDesc, int bufferSize) {
		super(resourceDesc);
		checkBufferSize(bufferSize);
		this.bufferSize = bufferSize;
		this.store = BufferStore.instance(bufferSize);
	}

	public void init(long length) {
		int buffers = 0;
		if (length % bufferSize == 0) {
			buffers = (int) (length / bufferSize);
		} else {
			buffers = (int) (length / bufferSize) + 1;
		}
		buffer = new Object[buffers];
	}

	private void checkBufferSize(int bufferSize) {
		if (bufferSize <= 0) {
			throw new IllegalArgumentException(
					"bufferSize must be greater than 0 (got " + bufferSize
							+ ")");
		}
	}

	@Override
	public void readBytes(byte[] b, int offset, int len) throws IOException {
		readBytes(b, offset, len, true);
	}

	@Override
	public void readBytes(byte[] b, int offset, int len, boolean useBuffer)
			throws IOException {
		if ((bufferIndex * bufferSize + bufferPosition + len) > length()) {
			throw new EOFException("read past EOF: " + this);
		}
		if (bufferPosition >= bufferSize) {
			bufferIndex++;
			bufferPosition = bufferPosition % bufferSize;
		}
		if (buffer[bufferIndex] == null) {
			refill(len);
			if (len <= (bufferSize - bufferPosition)) {
				if (len > 0) {
					System.arraycopy(buffer[bufferIndex], bufferPosition, b,
							offset, len);
				}
				bufferPosition += len;
			} else {
				int available = bufferSize - bufferPosition;
				if (available > 0) {
					System.arraycopy(buffer[bufferIndex], bufferPosition, b,
							offset, available);
					offset += available;
					len -= available;
					bufferPosition += available;
				}
				if (bufferPosition >= bufferSize) {
					bufferIndex++;
					bufferPosition = bufferPosition % bufferSize;
				}
				int size = len / bufferSize;
				int pos = len % bufferSize;
				for (int i = 0; i < size; i++) {
					System.arraycopy(buffer[bufferIndex], bufferPosition, b,
							offset, bufferSize);
					offset += bufferSize;
					len -= bufferSize;
					bufferIndex++;
				}
				if (pos > 0) {
					System.arraycopy(buffer[bufferIndex], bufferPosition, b,
							offset, pos);
					offset += pos;
					len -= pos;
					bufferPosition += pos;
				}
			}
		} else {
			if (len <= (bufferSize - bufferPosition)) {
				// the buffer contains enough data to satisfy this request
				if (len > 0) {
					System.arraycopy(buffer[bufferIndex], bufferPosition, b,
							offset, len);
				}
				bufferPosition += len;
			} else {
				// the buffer does not have enough data. First serve all we've
				// got.
				int available = bufferSize - bufferPosition;
				int oldPos = bufferPosition;
				int oldLen = len;
				if (available > 0) {
					System.arraycopy(buffer[bufferIndex], bufferPosition, b,
							offset, available);
					offset += available;
					len -= available;
					bufferPosition += available;
				}
				refill(len);
				int size = len / bufferSize;
				int pos = len % bufferSize;
				for (int i = 0; i < size; i++) {
					System.arraycopy(buffer[bufferIndex], bufferPosition, b,
							offset, bufferSize);
					offset += bufferSize;
					len -= bufferSize;
					bufferIndex++;
				}
				if (pos > 0) {
					try{
					    System.arraycopy(buffer[bufferIndex], bufferPosition, b,
	                            offset, pos);
					} catch (Exception e){
					    throw new RuntimeException("bufferLength=" + buffer.length + " bufferIndex=" + bufferIndex + " bufferPosition=" + bufferPosition + " bLengh=" + b.length + " offset=" + offset + " pos=" + pos + " oldPos=" + oldPos + " oldLen=" + oldLen);
					}
					offset += pos;
					len -= pos;
					bufferPosition += pos;
				}
			}
		}
	}

	@Override
	public int readInt() throws IOException {
		if ((bufferIndex * bufferSize + bufferPosition + 4) > length()) {
			throw new EOFException("read past EOF: " + this);
		}
		if (bufferPosition >= bufferSize) {
			bufferIndex++;
			bufferPosition = bufferPosition % bufferSize;
		}
		if (buffer[bufferIndex] == null) {
			refill(bufferSize - bufferPosition);
		}
		if ((bufferSize - bufferPosition) >= 4) {
			return ((((byte[]) buffer[bufferIndex])[bufferPosition++]
					& 0xFF) << 24)
					| ((((byte[]) buffer[bufferIndex])[bufferPosition++]
							& 0xFF) << 16)
					| ((((byte[]) buffer[bufferIndex])[bufferPosition++]
							& 0xFF) << 8)
					| (((byte[]) buffer[bufferIndex])[bufferPosition++] & 0xFF);
		} else {
			return super.readInt();
		}
	}

	@Override
	public long readLong() throws IOException {
		if ((bufferIndex * bufferSize + bufferPosition + 8) > length()) {
			throw new EOFException("read past EOF: " + this);
		}
		if (bufferPosition >= bufferSize) {
			bufferIndex++;
			bufferPosition = bufferPosition % bufferSize;
		}
		if (buffer[bufferIndex] == null) {
			refill(bufferSize - bufferPosition);
		}
		if (8 <= (bufferSize - bufferPosition)) {
			final int i1 = ((((byte[]) buffer[bufferIndex])[bufferPosition++]
					& 0xff) << 24)
					| ((((byte[]) buffer[bufferIndex])[bufferPosition++]
							& 0xff) << 16)
					| ((((byte[]) buffer[bufferIndex])[bufferPosition++]
							& 0xff) << 8)
					| (((byte[]) buffer[bufferIndex])[bufferPosition++] & 0xff);
			final int i2 = ((((byte[]) buffer[bufferIndex])[bufferPosition++]
					& 0xff) << 24)
					| ((((byte[]) buffer[bufferIndex])[bufferPosition++]
							& 0xff) << 16)
					| ((((byte[]) buffer[bufferIndex])[bufferPosition++]
							& 0xff) << 8)
					| (((byte[]) buffer[bufferIndex])[bufferPosition++] & 0xff);
			return (((long) i1) << 32) | (i2 & 0xFFFFFFFFL);
		} else {
			return super.readLong();
		}
	}

	@Override
	public int readVInt() throws IOException {
		if ((bufferIndex * bufferSize + bufferPosition + 5) > length()) {
			throw new EOFException("read past EOF: " + this);
		}
		if (bufferPosition >= bufferSize) {
			bufferIndex++;
			bufferPosition = bufferPosition % bufferSize;
		}
		if (buffer[bufferIndex] == null) {
			refill(bufferSize - bufferPosition);
		}
		if (5 <= (bufferSize - bufferPosition)) {
			byte b = ((byte[]) buffer[bufferIndex])[bufferPosition++];
			int i = b & 0x7F;
			for (int shift = 7; (b & 0x80) != 0; shift += 7) {
				b = ((byte[]) buffer[bufferIndex])[bufferPosition++];
				i |= (b & 0x7F) << shift;
			}
			return i;
		} else {
			return super.readVInt();
		}
	}

	@Override
	public long readVLong() throws IOException {
		if ((bufferIndex * bufferSize + bufferPosition + 9) > length()) {
			throw new EOFException("read past EOF: " + this);
		}
		if (bufferPosition >= bufferSize) {
			bufferIndex++;
			bufferPosition = bufferPosition % bufferSize;
		}
		if (buffer[bufferIndex] == null) {
			refill(bufferSize - bufferPosition);
		}
		if (9 <= bufferSize - bufferPosition) {
			byte b = ((byte[]) buffer[bufferIndex])[bufferPosition++];
			long i = b & 0x7F;
			for (int shift = 7; (b & 0x80) != 0; shift += 7) {
				b = ((byte[]) buffer[bufferIndex])[bufferPosition++];
				i |= (b & 0x7FL) << shift;
			}
			return i;
		} else {
			return super.readVLong();
		}
	}

	private void refill(long len) throws IOException {
		if (buffer != null) {
			synchronized (buffer) {
			    if (bufferPosition == bufferSize) {
			        bufferIndex++;
			        bufferPosition = 0;
			    }
			    int size = 0;
			    if ((len + bufferPosition) % bufferSize == 0) {
			        size = (int) ((len + bufferPosition) / bufferSize);
			    } else {
			        size = (int) ((len + bufferPosition) / bufferSize + 1);
			    }
			    long start = bufferIndex * bufferSize;
			    long end = start + bufferSize * size;
			    if (end > length()) {
			        end = length();
			    }
			    int newLength = (int) (end - start);
			    if (newLength <= 0) {
			        throw new EOFException("read past EOF: " + this);
			    }
			    size = newLength / bufferSize;
			    int pos = newLength % bufferSize;
				for (int i = 0; i < size; i++) {
					if (buffer[bufferIndex + i] == null) {
						byte[] bytes = store.takeBuffer(bufferSize);
						readInternal((bufferIndex + i) * bufferSize, bytes, 0,
								bufferSize);
						buffer[bufferIndex + i] = bytes;
					}
				}
				if (pos > 0) {
					if (buffer[bufferIndex + size] == null) {
						byte[] bytes = store.takeBuffer(bufferSize);
						readInternal((bufferIndex + size) * bufferSize, bytes,
								0, pos);
						buffer[bufferIndex + size] = bytes;
					}
				}
			}
		}
	}

	@Override
	public final void close() throws IOException {
		closeInternal();
		if (!this.clone) {
			for (int i = 0; i < buffer.length; i++) {
				if (buffer[i] != null) {
					store.putBuffer((byte[]) buffer[i]);
				}
			}
		}
		buffer = null;
	}

	protected abstract void closeInternal() throws IOException;

	/**
	 * Expert: implements buffer refill. Reads bytes from the current position
	 * in the input.
	 * 
	 * @param b
	 *            the array to read bytes into
	 * @param offset
	 *            the offset in the array to start storing bytes
	 * @param length
	 *            the number of bytes to read
	 */
	protected abstract void readInternal(long filePointer, byte[] b, int offset,
			int length) throws IOException;

	@Override
	public long getFilePointer() {
		return bufferIndex * bufferSize + bufferPosition;
	}

	@Override
	public void seek(long pos) throws IOException {
		if (pos >= length()) {
			throw new EOFException("seek past EOF: " + this);
		}
		bufferIndex = (int) (pos / bufferSize);
		bufferPosition = (int) (pos % bufferSize);
		if (buffer[bufferIndex] == null) {
			seekInternal(pos);
		}
	}

	/**
	 * Expert: implements seek. Sets current position in this file, where the
	 * next {@link #readInternal(byte[],int,int)} will occur.
	 * 
	 * @see #readInternal(byte[],int,int)
	 */
	protected abstract void seekInternal(long pos) throws IOException;

	@Override
	public IndexInput clone() {
		CustomBufferedIndexInput clone = (CustomBufferedIndexInput) super.clone();
		clone.clone = true;
		clone.buffer = this.buffer;
		clone.bufferPosition = this.bufferPosition;
		clone.bufferIndex = this.bufferIndex;
		clone.bufferSize = this.bufferSize;
		// System.out.println("Clone:" + this);
		return clone;
	}

	public boolean isClone() {
		return clone;
	}

	@Override
	public IndexInput slice(String sliceDescription, long offset, long length)
			throws IOException {
		return BufferedIndexInput.wrap(sliceDescription, this, offset, length);
	}

	/**
	 * Flushes the in-memory bufer to the given output, copying at most
	 * <code>numBytes</code>.
	 * <p>
	 * <b>NOTE:</b> this method does not refill the buffer, however it does
	 * advance the buffer position.
	 * 
	 * @return the number of bytes actually flushed from the in-memory buffer.
	 */
	protected int flushBuffer(IndexOutput out, long numBytes)
			throws IOException {
		int toCopy = bufferSize - bufferPosition;
		if (toCopy > numBytes) {
			toCopy = (int) numBytes;
		}
		if (toCopy > 0) {
			out.writeBytes((byte[]) buffer[bufferIndex], bufferPosition,
					toCopy);
			bufferPosition += toCopy;
		}
		return toCopy;
	}
}
