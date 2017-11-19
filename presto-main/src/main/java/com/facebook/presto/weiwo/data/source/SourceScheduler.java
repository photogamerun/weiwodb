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
package com.facebook.presto.weiwo.data.source;

import com.facebook.presto.spi.HostAddress;

import io.airlift.log.Logger;

public class SourceScheduler implements Runnable {

	private static final Logger log = Logger.get(SourceScheduler.class);

	WeiwoDataSourceManager sourceManager;
	private boolean isReadOnly;
	private boolean firstStart = true;

	public SourceScheduler(WeiwoDataSourceManager sourceManager,
			boolean isReadOnly) {
		this.sourceManager = sourceManager;
		this.isReadOnly = isReadOnly;
	}

	public SourceScheduler(WeiwoDataSourceManager sourceManager) {
		this(sourceManager, false);
	}

	public void schedule() {
		if (!sourceManager.tableCache.existTable()) {
			return;
		}
		if (isReadOnly) {
			return;
		}
		for (HostAddress address : sourceManager.getMissingNodes()) {
			sourceManager.processMissingNode(address);
		}
		for (String name : sourceManager.sourceHost.keySet()) {
			if ((System.currentTimeMillis() - sourceManager.sourceHost.get(name)
					.getLastTime()) > sourceManager.scheduleAbateTime
					&& sourceManager.getCurrentInstanceNum(
							name) < sourceManager.sourceCache.getSource(name)
									.getNodes()) {
				sourceManager.schedule(name);
			}
		}
	}

	@Override
	public void run() {
		while (true) {
			try {
				if (firstStart) {
					firstStart = false;
					Thread.sleep(2 * 60 * 1000);
				}
				schedule();
			} catch (Exception e) {
				log.error(e, "Source Scheduler error.");
			} finally {
				synchronized (this) {
					try {
						wait(1 * 60 * 1000);
					} catch (InterruptedException e) {
					}
				}
			}
		}
	}

	public void triggerSchedule() {
		synchronized (this) {
			notify();
		}
	}

}
