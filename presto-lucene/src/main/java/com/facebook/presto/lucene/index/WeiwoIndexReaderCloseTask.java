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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;

import io.airlift.log.Logger;

public class WeiwoIndexReaderCloseTask implements Runnable {

    private static final Logger log = Logger.get(WeiwoIndexReaderCloseTask.class);

    private long delay;
    ScheduledExecutorService excutor;
    WeiwoIndexReader weiwoIndexReader;

    public WeiwoIndexReaderCloseTask(WeiwoIndexReader weiwoIndexReader, long delay, ScheduledExecutorService excutor) {
        this.weiwoIndexReader = weiwoIndexReader;
        this.excutor = excutor;
        this.delay = delay;
    }

    @Override
    public void run() {
        if (weiwoIndexReader.getCurrentRef() <= 0) {
            log.info("Close index for : " + weiwoIndexReader.getPath());
            weiwoIndexReader.close();
        } else {
            if (delay > WeiwoDBConfigureKeys.CLOSE_DELAY_TIME || delay < 100) {
                delay = WeiwoDBConfigureKeys.CLOSE_DELAY_TIME;
            }
            excutor.schedule(new WeiwoIndexReaderCloseTask(weiwoIndexReader, delay, excutor), delay,
                    TimeUnit.MILLISECONDS);
        }
    }

}
