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
package com.facebook.presto.weiwo.table;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;

import org.I0Itec.zkclient.IZkChildListener;

import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.weiwo.manager.WeiwoZkClientFactory;

import io.airlift.log.Logger;

public class WeiwoTableCache {

    private static final Logger log = Logger.get(WeiwoTableCache.class);

    final WeiwoZkClientFactory zkClientFactory;
    final Map<String, Set<String>> dbTable;
    final Map<String, IZkChildListener> tablelisteners;
    final IZkChildListener dbListener;
    final String tablePath;
    final ServerConfig config;

    @Inject
    public WeiwoTableCache(WeiwoZkClientFactory zkClientFactory, ServerConfig config) {
        this.zkClientFactory = requireNonNull(zkClientFactory, "zkClientFactory is null");
        this.dbTable = new ConcurrentHashMap<>();
        this.config = requireNonNull(config, "config is null");
        this.tablePath = config.getZkPath() + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR
                + WeiwoDBConfigureKeys.WEIWO_TABLE_PATH;
        this.tablelisteners = new ConcurrentHashMap<>();
        this.dbListener = new DbChangeListener();
        init();
    }
    
    public boolean existTable(){
        if(dbTable.keySet().isEmpty()){
            return false;
        } else {
            for(Set<String> tables : dbTable.values()){
                if(!tables.isEmpty()){
                    return true;
                }
            }
            return false;
        }
    }

    private void init() {
        if (!zkClientFactory.getZkClient().exists(tablePath)) {
            zkClientFactory.getZkClient().createPersistent(tablePath, true);;
        }
        List<String> dbs = zkClientFactory.getZkClient().getChildren(tablePath);
        if (dbs != null) {
            dbs.forEach(p -> {
                List<String> dbTables = zkClientFactory.getZkClient()
                        .getChildren(tablePath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + p);
                Set<String> set = new HashSet<>();
                dbTable.put(p, set);
                if (dbTables != null) {
                    dbTables.forEach(table -> set.add(table));
                }
                TableChangeListener tableChangeListener = new TableChangeListener(p);
                zkClientFactory.getZkClient().subscribeChildChanges(
                        tablePath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + p, tableChangeListener);
                tablelisteners.put(p, tableChangeListener);
            });
        }
        zkClientFactory.getZkClient().subscribeChildChanges(tablePath, dbListener);
    }

    class DbChangeListener implements IZkChildListener {

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            log.info("DataBase Change ...");
            if (currentChilds != null) {
                for (String p : currentChilds) {
                    if (!dbTable.containsKey(p)) {
                        log.info("Add database name = " + p);
                        Set<String> set = new HashSet<>();
                        dbTable.put(p, set);
                        List<String> tables = zkClientFactory.getZkClient()
                                .getChildren(parentPath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + p);
                        if (tables != null) {
                            tables.forEach(table -> set.add(table));
                        }
                        TableChangeListener tableChangeListener = new TableChangeListener(p);
                        zkClientFactory.getZkClient().subscribeChildChanges(
                                parentPath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + p, tableChangeListener);
                        tablelisteners.put(p, tableChangeListener);
                    }
                }
                List<String> deletes = new ArrayList<>();
                dbTable.keySet().forEach(p -> {
                    if (!currentChilds.contains(p)) {
                        deletes.add(p);
                    }
                });
                for (String delete : deletes) {
                    log.info("Delete database name = " + delete);
                    zkClientFactory.getZkClient().unsubscribeChildChanges(
                            parentPath + WeiwoDBConfigureKeys.ZK_PATH_SEPARATOR + delete, tablelisteners.get(delete));
                    dbTable.remove(delete);
                }
            }
        }

    }

    class TableChangeListener implements IZkChildListener {

        public String dbName;

        public TableChangeListener(String dbName) {
            this.dbName = dbName;
        }

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            log.info("Table Change ... DB = " + dbName);
            if (currentChilds != null) {
                currentChilds.forEach(p -> {
                    if (!dbTable.get(dbName).contains(p)) {
                        log.info("Add Tabne name = " + dbName + "." + p);
                        dbTable.get(dbName).add(p);
                    }
                });
                Iterator<String> ir = dbTable.get(dbName).iterator();
                String table;
                while (ir.hasNext()) {
                    table = ir.next();
                    if (!currentChilds.contains(table)) {
                        log.info("Delete table name = " + dbName + "." + table);
                        ir.remove();
                    }
                }
            }
        }

    }

}
