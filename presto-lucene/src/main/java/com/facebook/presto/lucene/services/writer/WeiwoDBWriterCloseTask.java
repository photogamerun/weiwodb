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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;

import io.airlift.log.Logger;

/**
 * 当一个Split Writer达到需要关闭的阈值,在创建新的Split Writer之前需要commit并且关闭旧的Split Writer
 * 当该split分片的index reader有正在使用时,不能即刻关闭directory和reader,需要等待使用完毕 关闭操作交由可调度的线程池完成
 * 
 * @author folin01.chen
 *
 */
public class WeiwoDBWriterCloseTask implements Runnable {

    private static final Logger log = Logger.get(WeiwoDBWriterCloseTask.class);

    private long delay;
    ScheduledExecutorService excutor;
    WeiwoDBSplitWriter writer;

    public WeiwoDBWriterCloseTask(WeiwoDBSplitWriter writer, long delay, ScheduledExecutorService excutor) {
        this.writer = writer;
        this.excutor = excutor;
        this.delay = delay;
    }

    @Override
    public void run() {
        if (writer.getReaderRefCount() <= 0) {
            try {
                writer.closeMem();
            } catch (IOException e) {
                log.error(e, "Close WeiwoDBIndexWriter error.");
            }
        } else {
            if (delay > WeiwoDBConfigureKeys.CLOSE_DELAY_TIME || delay < 100) {
                delay = WeiwoDBConfigureKeys.CLOSE_DELAY_TIME;
            }
            excutor.schedule(new WeiwoDBWriterCloseTask(writer, delay, excutor), delay, TimeUnit.MILLISECONDS);
        }
    }

}
