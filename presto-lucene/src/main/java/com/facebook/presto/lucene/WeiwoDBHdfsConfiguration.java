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

import java.io.File;
import java.io.FileInputStream;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;

import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;
import com.google.common.base.Throwables;

import io.airlift.log.Logger;

public class WeiwoDBHdfsConfiguration {
    
    private static final Logger log = Logger.get(WeiwoDBHdfsConfiguration.class);
    
    public final Configuration conf;
    
    @Inject
    public WeiwoDBHdfsConfiguration(LuceneConfig config){
        this.conf = new Configuration();
        try{
            if(config.getResourceConfigFiles() != null && config.getResourceConfigFiles().size() > 0){
                for(String str : config.getResourceConfigFiles()){
                    log.info("Add resource : " + str);
                    conf.addResource(new FileInputStream(new File(str)));
                }
            }
        } catch (Exception e) {
            log.error("Create LuceneRecordCursor Error.", e);
            throw Throwables.propagate(e);
        }
        this.conf.setLong("dfs.blocksize", WeiwoDBConfigureKeys.RAM_BUFFER_SIZE_MB * 1024 * 1024);;
    }
    
    public Configuration getConfiguration(){
        return conf;
    }

}
