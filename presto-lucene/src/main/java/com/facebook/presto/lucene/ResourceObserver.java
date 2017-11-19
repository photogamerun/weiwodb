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

import java.util.Observable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.airlift.log.Logger;

/**
 * 
 * @author peter.wei
 *
 */
public class ResourceObserver extends Observable {

	private static final Logger log = Logger.get(ResourceObserver.class);

	private Resource<?> resource;
	private AtomicBoolean isStarted = new AtomicBoolean(false);
	private Object resourceInst = null;
	private Thread checkThread = null;

	public ResourceObserver(Resource<?> resource) {
		this.resource = resource;
	}

	public synchronized void start() {
		if (!isStarted()) {
			startTask();
		}
	}

	public synchronized void stop() {
		if (isStarted()) {
			stopTask();
		}
	}

	private void stopTask() {
		super.clearChanged();
		isStarted.compareAndSet(true, false);
		checkThread.interrupt();
		checkThread = null;
		resourceInst = null;
	}

	private void startTask() {
		isStarted.compareAndSet(false, true);
		checkThread = new Thread(new ObserveTask(), resource.toString());
		checkThread.setDaemon(true);
		checkThread.start();
	}

	private boolean isStarted() {
		return isStarted.get();
	}

	public class ObserveTask implements Runnable {

		private long checkInterval = 5000;

		AtomicBoolean isConnected = new AtomicBoolean(false);

		@Override
		public void run() {

			observe();
		}

		public void observe() {
			// first time startup
			connecting();
			fireConnected();

			while (isStarted()) {
				if (isConnectionAvailable()) {
					if (!isConnected()) {
						fireConnected();
					}
				} else {
					if (isConnected()) {
						connecting();
						fireConnected();
					}
				}
				try {
					TimeUnit.MILLISECONDS.sleep(checkInterval);
				} catch (InterruptedException e) {
					log.error(e);
					break;
				}
			}

			// last time stop
			disconnecting();
			fireDisConnected();
		}

		private void fireConnected() {
			if (resourceInst != null) {
				ResourceObserver.this.setChanged();
				notifyObservers(new ResourceEvent(resourceInst,
						ResourceEvent.TYPE_CONNECTED));
				isConnected.compareAndSet(false, true);
			} else {
				isConnected.compareAndSet(true, false);
			}
		}

		private void fireDisConnected() {
			if (resourceInst != null) {
				ResourceObserver.this.setChanged();
				notifyObservers(new ResourceEvent(resourceInst,
						ResourceEvent.TYPE_DISCONNECTED));
				isConnected.compareAndSet(true, false);
			} else {
				isConnected.compareAndSet(false, true);
			}
		}

		private void connecting() {
			try {
				resourceInst = resource.connect();
			} catch (Exception e) {
				log.error(e);
			}
		}

		private void disconnecting() {
			try {
				resource.close();
				resourceInst = null;
			} catch (Exception e) {
				log.error(e);
			}
		}

		private boolean isConnectionAvailable() {
			try {
				return resource.check();
			} catch (Exception e) {
				log.error(e);
			}
			return false;
		}

		private boolean isConnected() {
			return isConnected.get();
		}
	}
}