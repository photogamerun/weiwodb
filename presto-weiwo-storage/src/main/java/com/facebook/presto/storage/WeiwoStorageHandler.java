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
package com.facebook.presto.storage;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

/**
 * Created by wally.hong on 2017/2/15.
 */
public class WeiwoStorageHandler extends DefaultStorageHandler implements HiveMetaHook 
{
//	 private static Logger logger = LoggerFactory.getLogger(WeiwoStorageHandler.class.getName());
	public final static  String DEFAULT_PREFIX = "default";
	public final static String WEIWODB_SCHEMA_MAPPING_KEY = "weiwodb.schema.mapping";
	public final static String WEIWODB_DATA_LOCATION = "weiwodb.data.location";
	private Configuration jobConf; 

	@SuppressWarnings("rawtypes")
	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		return HiveInputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		return WeiwoOutputFormat.class;
	}

	@Override
	public HiveMetaHook getMetaHook() {
		return super.getMetaHook();
	}

	@Override
	public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
		super.configureInputJobProperties(tableDesc, jobProperties);
	}

	@Override
	public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
		super.configureJobConf(tableDesc, jobConf);
	}

	@Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    }

	@Override
	public void preCreateTable(Table table) {
	}

	@Override
	public void rollbackCreateTable(Table table) {

	}

	@Override
	public void commitCreateTable(Table table) {

	}

	@Override
	public void preDropTable(Table table) {

	}

	@Override
	public void rollbackDropTable(Table table) {

	}

	@Override
	public void commitDropTable(Table table, boolean b) {
	}

	public Configuration getJobConf() {
		return jobConf;
	}

	@Override
	public void setConf(Configuration conf) {
		jobConf = conf;
	}
}
