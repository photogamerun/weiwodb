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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.lucene.base.DataSource;
import com.facebook.presto.lucene.base.DataSourceAndNumber;
import com.facebook.presto.lucene.base.DataSourceNumber;
import com.facebook.presto.lucene.base.HeartBeatCommand;
import com.facebook.presto.lucene.base.HostDataSources;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.weiwo.table.WeiwoTableCache;

import io.airlift.log.Logger;

public class WeiwoDataSourceManager {

    private static final Logger log = Logger.get(WeiwoDataSourceManager.class);

    final WeiwoDataSourceCache sourceCache;
    final ReentrantReadWriteLock lock;
    final WriteLock writeLock;
    final ReadLock readLock;
    final NodeManager nodeManager;
    final WeiwoTableCache tableCache;
    final Map<String, SourceHostAddresses> sourceHost;
    final Map<HostAddress, List<DataSourceNumber>> hostSource;
    final Map<HostAddress, List<DataSourceNumber>> addPending;
    final Map<HostAddress, List<DataSourceNumber>> deletePending;
    final NodeSchedulerConfig nodeSchedulerConfig;
    public long scheduleAbateTime = 5 * 60 * 1000;
    public int maxSourceInstancePerNode = 4;
    public int allowConcurrentNumPerNode = 1;
    private SourceScheduler scheduler;
    private Thread scheduleThread;

    @Inject
    public WeiwoDataSourceManager(WeiwoDataSourceCache sourceCache, NodeManager nodeManager, WeiwoTableCache tableCache,
            NodeSchedulerConfig nodeSchedulerConfig,ServerConfig config) {
        this.sourceCache = requireNonNull(sourceCache, "sourceCache is null");
        this.tableCache = requireNonNull(tableCache, "tableCache is null");
        requireNonNull(config, "server config is null");
        this.nodeSchedulerConfig = requireNonNull(nodeSchedulerConfig, "nodeSchedulerConfig is null");
        this.lock = new ReentrantReadWriteLock(true);
        this.writeLock = lock.writeLock();
        this.readLock = lock.readLock();
        this.nodeManager = requireNonNull(nodeManager, "sourceCache is null");
        this.sourceHost = new HashMap<>();
        this.hostSource = new HashMap<>();
        this.addPending = new HashMap<>();
        this.deletePending = new HashMap<>();
        sourceCache.getSourcesName().forEach(name -> {
            sourceHost.put(name, new SourceHostAddresses(name, new HashMap<>()));
        });
        this.scheduler = new SourceScheduler(this,config.isReadOnly());
        this.scheduleThread = new Thread(scheduler);
        scheduleThread.setName("Source Scheduler");
        scheduleThread.setDaemon(true);
        scheduleThread.start();
    }

    public List<HostAddress> getMissingNodes() {
        List<HostAddress> result = new ArrayList<>();
        Set<Node> actives = nodeManager.getNodes(NodeState.ACTIVE);
        for (HostAddress now : hostSource.keySet()) {
            if (!containHost(actives, now)) {
                result.add(now);
            }
        }
        return result;
    }

    private boolean containHost(Set<Node> nodes, HostAddress address) {
        if (nodes == null || address == null) {
            return false;
        } else {
            if (nodes.stream().filter(node -> node.getHostAndPort().equals(address)).count() == 0) {
                return false;
            } else {
                return true;
            }
        }
    }

    public void processMissingNode(HostAddress address) {
        log.info("Node not in active , delete all instance on this node = " + address);
        writeLock.lock();
        try {
            List<DataSourceNumber> ms = hostSource.remove(address);
            addPending.remove(address);
            deletePending.remove(address);
            if (ms != null) {
                for (DataSourceNumber dsn : ms) {
                    sourceHost.get(dsn.getName()).getAddresses().remove(address);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void schedule(String sourceName) {
        writeLock.lock();
        try {
            List<Node> remainNodes;
            if (sourceCache.getSource(sourceName).isAllowConcurrentPerNode()) {
                remainNodes = nodeManager.getNodes(NodeState.ACTIVE).stream()
                        .filter(node -> (getHostInstanceNum(node.getHostAndPort()) < maxSourceInstancePerNode)
                                && getSourceCurrentInstanceNumForNode(sourceName,
                                        node.getHostAndPort()) < allowConcurrentNumPerNode)
                        .filter(node -> nodeSchedulerConfig.isIncludeCoordinator()
                                || !nodeManager.getCoordinators().contains(node))
                        .filter(node -> nodeSchedulerConfig.isIncludeWeiwoManager()
                                || !nodeManager.getWeiwomanagers().contains(node))
                        .collect(Collectors.toList());
            } else {
                remainNodes = nodeManager.getNodes(NodeState.ACTIVE).stream()
                        .filter(node -> (getHostInstanceNum(node.getHostAndPort()) < maxSourceInstancePerNode)
                                && getSourceCurrentInstanceNumForNode(sourceName, node.getHostAndPort()) < 1)
                        .filter(node -> nodeSchedulerConfig.isIncludeCoordinator()
                                || !nodeManager.getCoordinators().contains(node))
                        .filter(node -> nodeSchedulerConfig.isIncludeWeiwoManager()
                                || !nodeManager.getWeiwomanagers().contains(node))
                        .collect(Collectors.toList());
            }
            if (remainNodes != null) {
                for (Node node : remainNodes) {
                    Set<Integer> used = getUsedNumber(sourceName, node.getHostAndPort());
                    List<Integer> remains = getRemainingNumberList(used);
                    if (remains.size() > 0) {
                        log.info("Add a data source instance to " + node.getHostAndPort() + ". source name : "
                                + sourceName);
                        List<DataSourceNumber> dsns = addPending.get(node.getHostAndPort());
                        if (dsns == null) {
                            dsns = new ArrayList<>();
                            addPending.put(node.getHostAndPort(), dsns);
                        }
                        dsns.add(new DataSourceNumber(sourceName, remains.get(0)));
                    }
                }
            } else {
                log.warn("No remaining node to schedule to start instance. source name = " + sourceName);
            }
            sourceHost.get(sourceName).setLastTime(System.currentTimeMillis());
        } finally {
            writeLock.unlock();
        }
    }

    private List<Integer> getRemainingNumberList(Set<Integer> used) {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < allowConcurrentNumPerNode; i++) {
            list.add(i);
        }
        list.removeAll(used);
        return list;
    }

    private Set<Integer> getUsedNumber(String sourceName, HostAddress address) {
        Set<Integer> now = new HashSet<>();
        List<DataSourceNumber> list = hostSource.get(address);
        List<DataSourceNumber> pends = addPending.get(address);
        List<DataSourceNumber> deletes = deletePending.get(address);
        if (list != null) {
            Iterator<DataSourceNumber> hir = list.iterator();
            DataSourceNumber dsn;
            while (hir.hasNext()) {
                dsn = hir.next();
                if (dsn.getName().equals(sourceName)) {
                    now.add(dsn.getNumber());
                }
            }
        }
        if (pends != null) {
            Iterator<DataSourceNumber> air = pends.iterator();
            DataSourceNumber dsn;
            while (air.hasNext()) {
                dsn = air.next();
                if (dsn.equals(sourceName)) {
                    now.add(dsn.getNumber());
                }
            }
        }
        if (deletes != null) {
            Iterator<DataSourceNumber> dir = deletes.iterator();
            DataSourceNumber dsn;
            while (dir.hasNext()) {
                dsn = dir.next();
                if (dsn.equals(sourceName)) {
                    now.add(dsn.getNumber());
                }
            }
        }
        return now;
    }

    public void addDataSource(DataSource source) throws IOException {
        writeLock.lock();
        try {
            sourceCache.addSource(source);
            sourceHost.put(source.getName(), new SourceHostAddresses(source.getName(), new HashMap<>()));
        } finally {
            writeLock.unlock();
        }
    }

    public boolean deleteDataSource(String sourceName) throws IOException {
        writeLock.lock();
        try {
            if (sourceCache.existSource(sourceName)) {
                SourceHostAddresses sha = sourceHost.remove(sourceName);
                sourceCache.deleteSource(sha.getName());
                sha.getAddresses().keySet().forEach(ha -> {
                    if (hostSource.get(ha) != null) {
                        Iterator<DataSourceNumber> hir = hostSource.get(ha).iterator();
                        while (hir.hasNext()) {
                            if (hir.next().getName().equals(sourceName)) {
                                hir.remove();
                            }
                        }
                    }
                    if (addPending.get(ha) != null) {
                        Iterator<DataSourceNumber> air = addPending.get(ha).iterator();
                        while (air.hasNext()) {
                            if (air.next().getName().equals(sourceName)) {
                                air.remove();
                            }
                        }
                    }
                    if (sha.getAddresses().get(ha) != null && sha.getAddresses().get(ha).size() > 0) {
                        List<DataSourceNumber> needs = sha.getAddresses().get(ha);
                        List<DataSourceNumber> list = deletePending.get(ha);
                        if (list == null) {
                            list = new ArrayList<>();
                            deletePending.put(ha, list);
                        }
                        for (DataSourceNumber need : needs) {
                            if (!list.contains(need)) {
                                list.add(need);
                            }
                        }
                    }
                });
                return true;
            } else {
                return false;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public HeartBeatCommand heartBeat(HostDataSources hostDataSources) {
        process(hostDataSources);
        readLock.lock();
        List<DataSourceAndNumber> addL = new ArrayList<>();
        List<DataSourceAndNumber> deleteL = new ArrayList<>();
        try {
            List<DataSourceNumber> adds = addPending.get(hostDataSources.getHostAddress());
            if (adds != null) {
                Iterator<DataSourceNumber> it = adds.iterator();
                while (it.hasNext()) {
                    DataSourceNumber r = it.next();
                    if (sourceCache.getSource(r.getName()) == null) {
                        it.remove();
                    } else {
                        addL.add(new DataSourceAndNumber(sourceCache.getSource(r.getName()), r));
                    }
                }
            }
            List<DataSourceNumber> deletes = deletePending.get(hostDataSources.getHostAddress());
            if (deletes != null) {
                deletes.forEach(dsn -> {
                    deleteL.add(new DataSourceAndNumber(sourceCache.getSource(dsn.getName()), dsn));
                });
            }
            return new HeartBeatCommand(addL, deleteL);
        } finally {
            readLock.unlock();
        }
    }

    private void process(HostDataSources hostDataSources) {
        writeLock.lock();
        try {
            HostAddress address = hostDataSources.getHostAddress();
            List<DataSourceNumber> sources = hostDataSources.getSources();
            List<DataSourceNumber> adds = hostDataSources.getAdds();
            List<DataSourceNumber> deletes = hostDataSources.getDeletes();
            hostSource.put(address, sources);
            for (DataSourceNumber dsn : sources) {
                if (sourceCache.existSource(dsn.getName())) {
                    SourceHostAddresses current = sourceHost.get(dsn.getName());
                    if (current == null) {
                        current = new SourceHostAddresses(dsn.getName(), new HashMap<>());
                        List<DataSourceNumber> list = new ArrayList<>();
                        list.add(dsn);
                        current.getAddresses().put(address, list);
                        sourceHost.put(dsn.getName(), current);
                    } else {
                        if (current.getAddresses().get(address) == null) {
                            List<DataSourceNumber> list = new ArrayList<>();
                            list.add(dsn);
                            current.getAddresses().put(address, list);
                        } else {
                            if (!current.getAddresses().get(address).contains(dsn)) {
                                current.getAddresses().get(address).add(dsn);
                            }
                        }
                    }
                } else {
                    List<DataSourceNumber> list = deletePending.get(address);
                    if (list == null) {
                        list = new ArrayList<>();
                        deletePending.put(address, list);
                    }
                    list.add(dsn);
                }
            }
            for (DataSourceNumber dsn : adds) {
                if (addPending.get(address) != null) {
                    addPending.get(address).remove(dsn);
                }
            }
            for (DataSourceNumber dsn : deletes) {
                if (deletePending.get(address) != null) {
                    deletePending.remove(dsn);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public List<DataSource> listDataSource() {
        readLock.lock();
        List<DataSource> result = new ArrayList<>();
        try {
            result.addAll(sourceCache.getSources());
            return result;
        } finally {
            readLock.unlock();
        }
    }

    public DataSource getDataSource(String sourceName) throws IOException {
        readLock.lock();
        try {
            if (sourceCache.existSource(sourceName)) {
                return sourceCache.getSource(sourceName);
            } else {
                throw new IOException("Source not found.");
            }
        } finally {
            readLock.unlock();
        }
    }

    public int getHostInstanceNum(HostAddress address) {
        List<DataSourceNumber> list = hostSource.get(address);
        List<DataSourceNumber> pends = addPending.get(address);
        if (list == null && pends == null) {
            return 0;
        } else {
            int count = 0;
            if (list != null) {
                count = count + list.size();
            }
            if (pends != null) {
                count = count + pends.size();
            }
            return count;
        }
    }

    public int getCurrentInstanceNum(String sourceName) {
        SourceHostAddresses sha = sourceHost.get(sourceName);
        int count = 0;
        for (HostAddress address : sha.getAddresses().keySet()) {
            count = count + sha.getAddresses().get(address).size();
            List<DataSourceNumber> list = addPending.get(address);
            if (list != null) {
                Iterator<DataSourceNumber> ir = list.iterator();
                while (ir.hasNext()) {
                    if (ir.next().getName().equals(sourceName)) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    public int getSourceCurrentInstanceNumForNode(String sourceName, HostAddress address) {
        int count = 0;
        List<DataSourceNumber> list = hostSource.get(address);
        List<DataSourceNumber> pends = addPending.get(address);
        if (list != null) {
            Iterator<DataSourceNumber> hir = list.iterator();
            while (hir.hasNext()) {
                if (hir.next().getName().equals(sourceName)) {
                    count++;
                }
            }
        }
        if (pends != null) {
            Iterator<DataSourceNumber> air = pends.iterator();
            while (air.hasNext()) {
                if (air.next().getName().equals(sourceName)) {
                    count++;
                }
            }
        }
        return count;
    }

}
