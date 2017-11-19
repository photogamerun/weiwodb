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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Observer;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeState;

public class NodeStateChangeService {

    Map<String, NodeObservable> nodes;
    final ReentrantReadWriteLock lock;
    final WriteLock writeLock;
    final ReadLock readLock;
    InternalNodeManager nodeManager;
    public long interval = 5 * 1000;

    @Inject
    public NodeStateChangeService(InternalNodeManager nodeManager) {
        this.nodes = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.writeLock = lock.writeLock();
        this.readLock = lock.readLock();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        Timer timer = new Timer("HeartBeatService Thread", true);
        timer.schedule(new CheckState(), interval, interval);
    }

    public void statucCheck() {
        Set<Node> wnodes = nodeManager.getNodes(NodeState.ACTIVE);
        List<String> result = wnodes.stream().map(node -> node.getNodeIdentifier()).collect(Collectors.toList());
        List<String> current = new ArrayList<>();
        current.addAll(nodes.keySet());
        current.removeAll(result);
        for (String nodeId : current) {
            processNode(nodeId);
        }
    }

    public Node getNotActiveNodeById(String nodeId) {
        Set<Node> snodes = nodeManager.getNodes(NodeState.SHUTTING_DOWN);
        List<Node> remain = snodes.stream().filter(node -> node.getNodeIdentifier().equals(nodeId))
                .collect(Collectors.toList());
        if (remain != null && remain.size() > 0) {
            return remain.get(0);
        }
        Set<Node> inodes = nodeManager.getNodes(NodeState.INACTIVE);
        remain = inodes.stream().filter(node -> node.getNodeIdentifier().equals(nodeId)).collect(Collectors.toList());
        if (remain != null && remain.size() > 0) {
            return remain.get(0);
        }
        return null;
    }

    private void processNode(String nodeId) {
        Node node = getNotActiveNodeById(nodeId);
        NodeObservable n = nodes.get(nodeId);
        if (n != null) {
            writeLock.lock();
            try {
                n.nodeNotAvailable(node);
                n.deleteObservers();
                nodes.remove(nodeId);
            } finally {
                writeLock.unlock();
            }
        }
    }

    public void addObserver(String nodeId, Observer server) {
        writeLock.lock();
        try {
            NodeObservable node = nodes.get(nodeId);
            if (node == null) {
                node = new NodeObservable();
                nodes.put(nodeId, node);
            }
            node.addObserver(server);
        } finally {
            writeLock.unlock();
        }
    }

    public void clear(String nodeId) {
        NodeObservable n = nodes.get(nodeId);
        if (n != null) {
            writeLock.lock();
            try {
                n.clear(nodeId);
                n.deleteObservers();
                nodes.remove(nodeId);
            } finally {
                writeLock.unlock();
            }
        }
    }

    public void deleteObserver(String nodeId, Observer server) {
        writeLock.lock();
        try {
            NodeObservable node = nodes.get(nodeId);
            if (node != null) {
                node.deleteObserver(server);
            }
        } finally {
            writeLock.unlock();
        }
    }

    class CheckState extends TimerTask {

        @Override
        public void run() {
            statucCheck();
        }

    }

}
