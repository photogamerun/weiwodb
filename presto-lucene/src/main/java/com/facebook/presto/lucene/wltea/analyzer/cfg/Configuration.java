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
package com.facebook.presto.lucene.wltea.analyzer.cfg;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Properties;

import io.airlift.log.Logger;
import scala.reflect.generic.Trees.This;

public class Configuration {

	private static String FILE_NAME = "IKAnalyzer.cfg.xml";
	private static final String EXT_DICT = "ext_dict";
	private static final String REMOTE_EXT_DICT = "remote_ext_dict";
	private static final String EXT_STOP = "ext_stopwords";
	private static final String REMOTE_EXT_STOP = "remote_ext_stopwords";
	public static Logger logger = Logger.get(Configuration.class);
	public static String conf_dir;
	private Properties props;
	private static Configuration config;

	public Configuration() {
		props = new Properties();

		Configuration.conf_dir = conf_dir + File.separator + "config";
		
		String configFile = conf_dir + File.separator + FILE_NAME;

		InputStream input = null;
		try {
			logger.info("try load config from {" + configFile + "}");
			input = new FileInputStream(new File(configFile));
		} catch (FileNotFoundException e) {
		  logger.error("ik-analyzer", e);
		}
		if (input != null) {
			try {
				props.loadFromXML(input);
			} catch (InvalidPropertiesFormatException e) {
				logger.error("ik-analyzer", e);
			} catch (IOException e) {
				logger.error("ik-analyzer", e);
			}
		}
	}

	public List<String> getExtDictionarys() {
		List<String> extDictFiles = new ArrayList<String>(2);
		String extDictCfg = props.getProperty(EXT_DICT);
		if (extDictCfg != null) {

			String[] filePaths = extDictCfg.split(";");
			if (filePaths != null) {
				for (String filePath : filePaths) {
					if (filePath != null && !"".equals(filePath.trim())) {
						String file = filePath.trim();
						extDictFiles.add(file.toString());
					}
				}
			}
		}
		return extDictFiles;
	}

	public List<String> getRemoteExtDictionarys() {
		List<String> remoteExtDictFiles = new ArrayList<String>(2);
		String remoteExtDictCfg = props.getProperty(REMOTE_EXT_DICT);
		if (remoteExtDictCfg != null) {

			String[] filePaths = remoteExtDictCfg.split(";");
			if (filePaths != null) {
				for (String filePath : filePaths) {
					if (filePath != null && !"".equals(filePath.trim())) {
						remoteExtDictFiles.add(filePath);

					}
				}
			}
		}
		return remoteExtDictFiles;
	}

	public List<String> getExtStopWordDictionarys() {
		List<String> extStopWordDictFiles = new ArrayList<String>(2);
		String extStopWordDictCfg = props.getProperty(EXT_STOP);
		if (extStopWordDictCfg != null) {

			String[] filePaths = extStopWordDictCfg.split(";");
			if (filePaths != null) {
				for (String filePath : filePaths) {
					if (filePath != null && !"".equals(filePath.trim())) {
						String file = filePath.trim();
						extStopWordDictFiles.add(file.toString());

					}
				}
			}
		}
		return extStopWordDictFiles;
	}

	public List<String> getRemoteExtStopWordDictionarys() {
		List<String> remoteExtStopWordDictFiles = new ArrayList<String>(2);
		String remoteExtStopWordDictCfg = props.getProperty(REMOTE_EXT_STOP);
		if (remoteExtStopWordDictCfg != null) {

			String[] filePaths = remoteExtStopWordDictCfg.split(";");
			if (filePaths != null) {
				for (String filePath : filePaths) {
					if (filePath != null && !"".equals(filePath.trim())) {
						remoteExtStopWordDictFiles.add(filePath);

					}
				}
			}
		}
		return remoteExtStopWordDictFiles;
	}

	public String getDictRoot() {
		return conf_dir;
	}
	
	public synchronized static Configuration getConfiguration(){
	  if(config == null){
	    config = new Configuration();
	  }
	  return config;
	}
	
}
