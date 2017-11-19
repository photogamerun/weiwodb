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

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.core.Response.Status;

import com.facebook.presto.lucene.base.IndexInfoWithNodeInfoSimple;
import com.facebook.presto.lucene.services.writer.exception.WriterRegisterException;
import com.facebook.presto.lucene.util.JsonCodecUtils;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;

import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.json.JsonCodec;
/**
 * restful client access indexManger restful service
 * 
 * @author peter.wei
 *
 */
public class IndexManagerClient {

	private LuceneConfig config;

	private JsonCodec<List<IndexInfoWithNodeInfoSimple>> indexInfoListCodec;
	private JsonCodec<IndexInfoWithNodeInfoSimple> indexInfoCodec;

	private HttpClient httpClient;

	public IndexManagerClient(LuceneConfig config, HttpClient httpClient) {
		this.config = config;
		this.httpClient = httpClient;
		this.indexInfoListCodec = JsonCodecUtils.getIndexInfoListCodec();
		this.indexInfoCodec = JsonCodecUtils.getIndexInfoCodec();
	}

	public List<IndexInfoWithNodeInfoSimple> listIndex(String db, String table,
			String[] partitions) {
		HttpUriBuilder uriBuilder = uriBuilderFrom(
				URI.create(config.getWeiwomanagerUri()));
		uriBuilder.appendPath("/v1/index/list");
		List<String> ps = Arrays.asList(partitions);
		uriBuilder.addParameter("db", db);
		uriBuilder.addParameter("table", table);
		uriBuilder.addParameter("partitions", ps);
		Request request = prepareGet().setUri(uriBuilder.build())
				.setHeader(HttpHeaders.CONTENT_TYPE,
						MediaType.JSON_UTF_8.toString())
				.build();
		JsonResponse<List<IndexInfoWithNodeInfoSimple>> res = httpClient
				.execute(request,
						createFullJsonResponseHandler(indexInfoListCodec));
		if (res.getStatusCode() != Status.OK.getStatusCode()) {
			throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
					res.getException());
		}
		return indexInfoListCodec.fromJson(res.getResponseBody());
	}

	public List<IndexInfoWithNodeInfoSimple> listIndexWriter(String db,
			String table, String[] partitions) {
		HttpUriBuilder uriBuilder = uriBuilderFrom(
				URI.create(config.getWeiwomanagerUri()));
		uriBuilder.appendPath("/v1/index/list/writer");
		List<String> ps = Arrays.asList(partitions);
		uriBuilder.addParameter("db", db);
		uriBuilder.addParameter("table", table);
		uriBuilder.addParameter("partitions", ps);
		Request request = prepareGet().setUri(uriBuilder.build())
				.setHeader(HttpHeaders.CONTENT_TYPE,
						MediaType.JSON_UTF_8.toString())
				.build();
		JsonResponse<List<IndexInfoWithNodeInfoSimple>> res = httpClient
				.execute(request,
						createFullJsonResponseHandler(indexInfoListCodec));
		if (res.getStatusCode() != Status.OK.getStatusCode()) {
			throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
					res.getException());
		}
		return indexInfoListCodec.fromJson(res.getResponseBody());
	}

	public boolean register(IndexInfoWithNodeInfoSimple indexInfoWithNode) {
		HttpUriBuilder uriBuilder = uriBuilderFrom(
				URI.create(config.getWeiwomanagerUri()));
		uriBuilder.appendPath("/v1/index/open");
		Request request = preparePost().setUri(uriBuilder.build())
				.setHeader(HttpHeaders.CONTENT_TYPE,
						MediaType.JSON_UTF_8.toString())
				.setBodyGenerator(
						jsonBodyGenerator(indexInfoCodec, indexInfoWithNode))
				.build();
		StatusResponse res = httpClient.execute(request,
				createStatusResponseHandler());
		if (res.getStatusCode() != Status.OK.getStatusCode()) {
			return false;
		} else {
			return true;
		}
	}

	public boolean close(IndexInfoWithNodeInfoSimple indexInfoWithNode) {

		HttpUriBuilder uriBuilder = uriBuilderFrom(
				URI.create(config.getWeiwomanagerUri()));
		uriBuilder.appendPath("/v1/index/close");
		Request request = preparePost().setUri(uriBuilder.build())
				.setHeader(HttpHeaders.CONTENT_TYPE,
						MediaType.JSON_UTF_8.toString())
				.setBodyGenerator(
						jsonBodyGenerator(indexInfoCodec, indexInfoWithNode))
				.build();
		StatusResponse res = httpClient.execute(request,
				createStatusResponseHandler());
		if (res.getStatusCode() != Status.OK.getStatusCode()) {
			return false;
		} else {
			return true;
		}
	}

	public void unRegisterWriter(IndexInfoWithNodeInfoSimple indexInfoWithNode)
			throws WriterRegisterException {

		HttpUriBuilder uriBuilder = uriBuilderFrom(
				URI.create(config.getWeiwomanagerUri()));
		uriBuilder.appendPath("/v1/index/writer/close");
		Request request = preparePost().setUri(uriBuilder.build())
				.setHeader(HttpHeaders.CONTENT_TYPE,
						MediaType.JSON_UTF_8.toString())
				.setBodyGenerator(
						jsonBodyGenerator(indexInfoCodec, indexInfoWithNode))
				.build();
		StatusResponse res = httpClient.execute(request,
				createStatusResponseHandler());
		if (res.getStatusCode() != Status.OK.getStatusCode()) {
			throw new WriterRegisterException(
					new IOException("UnRegister writer failed. status code = "
							+ res.getStatusCode() + ",status message = "
							+ res.getStatusMessage()));
		}
	}

	public void registerWriter(IndexInfoWithNodeInfoSimple indexInfoWithNode)
			throws IOException {
		HttpUriBuilder uriBuilder = uriBuilderFrom(
				URI.create(config.getWeiwomanagerUri()));
		uriBuilder.appendPath("/v1/index/writer");
		Request request = preparePost().setUri(uriBuilder.build())
				.setHeader(HttpHeaders.CONTENT_TYPE,
						MediaType.JSON_UTF_8.toString())
				.setBodyGenerator(
						jsonBodyGenerator(indexInfoCodec, indexInfoWithNode))
				.build();
		StatusResponse res = httpClient.execute(request,
				createStatusResponseHandler());
		if (res.getStatusCode() != Status.OK.getStatusCode()) {
			throw new WriterRegisterException(
					new IOException("Register writer failed. status code = "
							+ res.getStatusCode() + ",message="
							+ res.getStatusMessage()));
		}
	}

}