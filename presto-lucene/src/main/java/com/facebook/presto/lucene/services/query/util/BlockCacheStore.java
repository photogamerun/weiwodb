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
package com.facebook.presto.lucene.services.query.util;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.facebook.presto.lucene.services.query.util.DocIdsList.Block;

public class BlockCacheStore {

	private volatile int cacheSize = 4096;

	private volatile int minSize = 1024;

	private ConcurrentLinkedQueue<Block> queue;

	private AtomicInteger size = new AtomicInteger(0);

	private long lastTime = 0;

	static boolean inited = false;
	static Object object = new Object();

	private static BlockCacheStore instance;

	public static BlockCacheStore instance() {
		if (instance == null) {
			synchronized (object) {
				if (!inited) {
					instance = new BlockCacheStore();
					inited = true;
				}
			}
		}
		return instance;
	}

	public BlockCacheStore() {
		setupCache(minSize);
	}

	public Block takeBlock() {
		Block block = queue.poll();
		if (block != null) {
			block.revert();
			size.decrementAndGet();
		} else {
			block = new Block();
		}
		lastTime = System.currentTimeMillis();
		return block;
	}

	public void putBlock(Block block) {
		if (size.get() < cacheSize) {
			queue.offer(block);
			size.incrementAndGet();
		}
	}

	class ClearTask implements Runnable {

		@Override
		public void run() {
			while (true) {
				synchronized (this) {
					try {
						wait(30000);
					} catch (InterruptedException e) {
					}
					if ((System.currentTimeMillis() - lastTime) > 3 * 60
							* 1000) {
						if (size.get() > minSize) {
							int clearSize = size.get() / 4;
							if ((size.get() - clearSize) < minSize) {
								clearSize = size.get() - minSize;
							}
							while (clearSize > 0 && (queue.poll()) != null) {
								clearSize--;
							}
						}
					}
				}
			}
		}

	}

	private void setupCache(int count) {
		ConcurrentLinkedQueue<Block> queue = new ConcurrentLinkedQueue<>();
		count = Math.min(count, cacheSize);
		for (int i = 0; i < count; i++) {
			queue.add(new Block());
		}
		size.addAndGet(count);
		this.queue = queue;
	}

}
