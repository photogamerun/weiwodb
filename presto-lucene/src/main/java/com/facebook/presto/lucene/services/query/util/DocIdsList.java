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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class DocIdsList {

	public static final int SIZE = 65536;

	final List<Block> blocks = new ArrayList<>();
	private Block currentBlock = null;
	private volatile int count = 0;
	private volatile int num = 0;
	final boolean all;
	BlockCacheStore store;

	public DocIdsList() {
		this.all = false;
		this.store = BlockCacheStore.instance();
	}

	public void release() {
		if (!all) {
			for (Block block : blocks) {
				store.putBlock(block);
			}
		}
	}

	public DocIdsList(int num) {
		this.all = true;
		this.num = num;
		this.count = num;
	}

	public DocIterator iterator() {
		return new DocIterator(all);
	}

	public class DocIterator {

		Iterator<Block> blocksItr = null;
		Block cBlock = null;
		volatile int current = 0;

		DocIterator(boolean all) {
			if (!all) {
				blocksItr = blocks.iterator();
			}
		}

		public boolean hasNext() {
			if (all) {
				if (current < num) {
					return true;
				} else {
					return false;
				}
			} else {
				if (cBlock == null) {
					if (blocksItr.hasNext()) {
						cBlock = blocksItr.next();
						cBlock.reset();
						if (cBlock.hasNext()) {
							return true;
						} else {
							return false;
						}
					} else {
						return false;
					}
				} else {
					if (cBlock.hasNext()) {
						return true;
					} else {
						if (blocksItr.hasNext()) {
							cBlock = blocksItr.next();
							cBlock.reset();
							if (cBlock.hasNext()) {
								return true;
							} else {
								return false;
							}
						} else {
							return false;
						}
					}
				}
			}
		}

		public int next() {
			if (all) {
				return current++;
			} else {
				return cBlock.next();
			}
		}
	}

	public void add(int doc) {
		if (currentBlock == null) {
			currentBlock = store.takeBlock();
			currentBlock.add(doc);
			blocks.add(currentBlock);
		} else {
			if (!currentBlock.add(doc)) {
				currentBlock = store.takeBlock();
				currentBlock.add(doc);
				blocks.add(currentBlock);
			}
		}
		count++;
	}

	public long size() {
		return count;
	}

	public static class Block {

		private int[] docIds;

		private volatile int index = 0;

		private volatile int iteral = 0;

		Block() {
			docIds = new int[SIZE];
		}

		private boolean add(int docId) {
			if (index >= SIZE) {
				return false;
			} else {
				docIds[index] = docId;
				index++;
				return true;
			}
		}

		private boolean hasNext() {
			return iteral < index;
		}

		private int next() {
			return docIds[iteral++];
		}

		private void reset() {
			iteral = 0;
		}

		public void revert() {
			index = 0;
			iteral = 0;
		}
	}

}
