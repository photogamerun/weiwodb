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
package com.facebook.presto.weiwo.manager.index;

import java.util.Observable;

import com.facebook.presto.spi.Node;

import io.airlift.log.Logger;

public class NodeObservable extends Observable {

    private static final Logger log = Logger.get(NodeObservable.class);

    public void nodeNotAvailable(Node node) {
        log.warn("Node state problem. Clear reader and writer info . node = " + node);
        setChanged();
        notifyObservers();
    }
    
    public void clear(String node) {
        log.warn("Clear node when node request. Clear reader and writer info . node = " + node);
        setChanged();
        notifyObservers();
    }

}
