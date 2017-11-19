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
package com.facebook.presto.lucene.wltea.analyzer.dic;

import java.io.IOException;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import io.airlift.log.Logger;

public class Monitor implements Runnable {

    public static Logger logger = Logger.get(Monitor.class);

	private static CloseableHttpClient httpclient = HttpClients.createDefault();
	/*
	 * 上次更改时间
	 */
	private String last_modified;
	/*
	 * 资源属性
	 */
	private String eTags;

	/*
	 * 请求地址
	 */
	private String location;

	public Monitor(String location) {
		this.location = location;
		this.last_modified = null;
		this.eTags = null;
	}
	/**
	 * 监控流程：
	 *  ①向词库服务器发送Head请求
	 *  ②从响应中获取Last-Modify、ETags字段值，判断是否变化
	 *  ③如果未变化，休眠1min，返回第①步
	 * 	④如果有变化，重新加载词典
	 *  ⑤休眠1min，返回第①步
	 */

	public void run() {

		//超时设置
		RequestConfig rc = RequestConfig.custom().setConnectionRequestTimeout(10*1000)
				.setConnectTimeout(10*1000).setSocketTimeout(15*1000).build();

		HttpHead head = new HttpHead(location);
		head.setConfig(rc);

		//设置请求头
		if (last_modified != null) {
			head.setHeader("If-Modified-Since", last_modified);
		}
		if (eTags != null) {
			head.setHeader("If-None-Match", eTags);
		}

		CloseableHttpResponse response = null;
		try {

			response = httpclient.execute(head);

			//返回200 才做操作
			if(response.getStatusLine().getStatusCode()==200){

				if (((response.getLastHeader("Last-Modified")!=null) && !response.getLastHeader("Last-Modified").getValue().equalsIgnoreCase(last_modified))
						||((response.getLastHeader("ETag")!=null) && !response.getLastHeader("ETag").getValue().equalsIgnoreCase(eTags))) {

					// 远程词库有更新,需要重新加载词典，并修改last_modified,eTags
					Dictionary.getSingleton().reLoadMainDict();
					last_modified = response.getLastHeader("Last-Modified")==null?null:response.getLastHeader("Last-Modified").getValue();
					eTags = response.getLastHeader("ETag")==null?null:response.getLastHeader("ETag").getValue();
				}
			}else if (response.getStatusLine().getStatusCode()==304) {
				//没有修改，不做操作
				//noop
			}else{
				Dictionary.logger.info("remote_ext_dict {" + location + "} return bad code {" + response.getStatusLine().getStatusCode() + "}");
			}

		} catch (Exception e) {
			Dictionary.logger.error("remote_ext_dict {" + location + "} error!",e);
		}finally{
			try {
				if (response != null) {
					response.close();
				}
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

}