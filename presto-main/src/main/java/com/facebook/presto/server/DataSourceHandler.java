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
package com.facebook.presto.server;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import com.facebook.presto.client.ClientTypeSignature;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.lucene.base.DataSource;
import com.facebook.presto.metadata.ForDataSourceManager;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;

import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;

public class DataSourceHandler {

	public static final ObjectMapperProvider p = new ObjectMapperProvider();
	public static final JsonCodec<List<DataSource>> DATASOURCE_LIST_CODEC = new JsonCodecFactory(
			p).listJsonCodec(DataSource.class);
	public static final JsonCodec<DataSource> DATASOURCE_CODEC = new JsonCodecFactory(
			p).jsonCodec(DataSource.class);

	private AtomicLong opCount = new AtomicLong();

	static String patterns = "(add|list|delete|get)\\s.?datasource";
	static Pattern pattern = Pattern.compile(patterns,
			Pattern.CASE_INSENSITIVE);

	private final ServerConfig config;
	private final HttpClient httpClient;

	@Inject
	public DataSourceHandler(ServerConfig config,
			@ForDataSourceManager HttpClient httpClient) {
		this.config = requireNonNull(config, "config is null");
		this.httpClient = requireNonNull(httpClient, "httpClient is null");
	}

	public static boolean isDatasourceSql(String statement) {
		Matcher find = pattern.matcher(statement.trim());
		return find.find();
	}

	public Response handle(String statement) {
		String[] strs = statement.split("datasource");
		String op = strs[0].trim();
		switch (op) {
			case "add" :
				String source = strs[1].trim();
				return addDatasource(source);
			case "list" :
				return listDatasource();
			case "delete" :
				String delete = strs[1].trim();
				return deleteDatasource(delete);
			case "get" :
				String name = strs[1].trim();
				return getDatasource(name);
			default :
				return buildDefault();
		}
	}

	public Response buildDefault() {
		StatementStats stats = new StatementStats("FAILED", false, false, 0, 0,
				0, 0, 0, 0L, 0L, 0L, 0L, 0L, null);
		QueryError error = new QueryError("Unrecognized Error", null, 0,
				"RECOGNIZED_ERROR", "0", null, null);
		QueryResults queryResults = new QueryResults(
				"data_source_op_" + opCount.getAndIncrement(),
				URI.create(config.getManagerUri()), null, null, null, null,
				stats, error, null, null);
		ResponseBuilder response = Response.ok(queryResults);
		return response.build();
	}

	public Response addDatasource(String datasource) {
		HttpUriBuilder uriBuilder = uriBuilderFrom(
				URI.create(config.getManagerUri()));
		uriBuilder.appendPath("/v1/datasource/add");
		Request request = preparePost().setUri(uriBuilder.build())
				.setHeader(HttpHeaders.CONTENT_TYPE,
						MediaType.JSON_UTF_8.toString())
				.setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(
						datasource, Charset.forName("UTF-8")))
				.build();
		StatusResponse res = httpClient.execute(request,
				createStatusResponseHandler());
		if (res.getStatusCode() != Status.OK.getStatusCode()) {
			StatementStats stats = new StatementStats("FAILED", false, false, 0,
					0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, null);
			QueryError error = new QueryError(res.getStatusMessage(), null,
					res.getStatusCode(), "ADD_HTTP_ERROR",
					res.getStatusCode() + "", null, null);
			QueryResults queryResults = new QueryResults(
					"data_source_add_" + opCount.getAndIncrement(),
					URI.create(config.getManagerUri()), null, null, null, null,
					stats, error, "ADD DATASOURCE", null);
			ResponseBuilder response = Response.ok(queryResults);
			return response.build();
		} else {
			StatementStats stats = new StatementStats("FINISHED", false, false,
					0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, null);
			Column column = new Column("result", "boolean",
					new ClientTypeSignature(StandardTypes.BOOLEAN,
							ImmutableList.of()));
			List<Object> data = ImmutableList.of(true);
			QueryResults queryResults = new QueryResults(
					"data_source_add_" + opCount.getAndIncrement(),
					URI.create(config.getManagerUri()), null, null,
					ImmutableList.of(column), ImmutableList.of(data), stats,
					null, "ADD DATASOURCE", null);
			ResponseBuilder response = Response.ok(queryResults);
			return response.build();
		}
	}

	public Response listDatasource() {
		HttpUriBuilder uriBuilder = uriBuilderFrom(
				URI.create(config.getManagerUri()));
		uriBuilder.appendPath("/v1/datasource/list");
		Request request = prepareGet().setUri(uriBuilder.build())
				.setHeader(HttpHeaders.CONTENT_TYPE,
						MediaType.JSON_UTF_8.toString())
				.build();
		JsonResponse<List<DataSource>> res = httpClient.execute(request,
				createFullJsonResponseHandler(DATASOURCE_LIST_CODEC));
		if (res.getStatusCode() != Status.OK.getStatusCode()) {
			StatementStats stats = new StatementStats("FAILED", false, false, 0,
					0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, null);
			QueryError error = new QueryError(res.getStatusMessage(), null,
					res.getStatusCode(), "LIST DATASOURCE ERROR",
					res.getStatusCode() + "", null, null);
			QueryResults queryResults = new QueryResults(
					"data_source_list_" + opCount.getAndIncrement(),
					URI.create(config.getManagerUri()), null, null, null, null,
					stats, error, "LIST DATASOURCE ERROR", null);
			ResponseBuilder response = Response.ok(queryResults);
			return response.build();
		}
		List<DataSource> result = DATASOURCE_LIST_CODEC
				.fromJson(res.getResponseBody());
		StatementStats stats = new StatementStats("FINISHED", false, false, 0,
				0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, null);
		Column column = new Column("name", "varchar", new ClientTypeSignature(
				StandardTypes.VARCHAR, ImmutableList.of()));
		List<Object> data = result.stream().map(ds -> (Object) ds.getName())
				.collect(Collectors.toList());
		List<List<Object>> list = null;
		if (data != null) {
			list = data.stream().map(obj -> ImmutableList.of(obj))
					.collect(Collectors.toList());
		} else {
			list = ImmutableList.of();
		}
		QueryResults queryResults = new QueryResults(
				"data_source_list_" + opCount.getAndIncrement(),
				URI.create(config.getManagerUri()), null, null,
				ImmutableList.of(column), list, stats, null, null, 0L);
		ResponseBuilder response = Response.ok(queryResults);
		return response.build();
	}

	public Response getDatasource(String name) {
		HttpUriBuilder uriBuilder = uriBuilderFrom(
				URI.create(config.getManagerUri()));
		uriBuilder.appendPath("/v1/datasource/get/" + name);
		Request request = prepareGet().setUri(uriBuilder.build())
				.setHeader(HttpHeaders.CONTENT_TYPE,
						MediaType.JSON_UTF_8.toString())
				.build();
		JsonResponse<DataSource> res = httpClient.execute(request,
				createFullJsonResponseHandler(DATASOURCE_CODEC));
		if (res.getStatusCode() != Status.OK.getStatusCode()) {
			StatementStats stats = new StatementStats("FAILED", false, false, 0,
					0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, null);
			QueryError error = new QueryError(res.getStatusMessage(), null,
					res.getStatusCode(), "GET DATASOURCE ERROR",
					res.getStatusCode() + "", null, null);
			QueryResults queryResults = new QueryResults(
					"data_source_get_" + opCount.getAndIncrement(),
					URI.create(config.getManagerUri()), null, null, null, null,
					stats, error, "GET DATASOURCE ERROR", null);
			ResponseBuilder response = Response.ok(queryResults);
			return response.build();
		}
		DataSource result = DATASOURCE_CODEC.fromJson(res.getResponseBody());
		if (result != null) {
			StatementStats stats = new StatementStats("FINISHED", false, false,
					0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, null);
			Column column = new Column("datasource", "varchar",
					new ClientTypeSignature(StandardTypes.VARCHAR,
							ImmutableList.of()));
			QueryResults queryResults = new QueryResults(
					"data_source_list_" + opCount.getAndIncrement(),
					URI.create(config.getManagerUri()), null, null,
					ImmutableList.of(column),
					ImmutableList.of(ImmutableList.of(buildDsResult(result))),
					stats, null, null, 0L);
			ResponseBuilder response = Response.ok(queryResults);
			return response.build();
		} else {
			StatementStats stats = new StatementStats("FINISHED", false, false,
					0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, null);
			QueryResults queryResults = new QueryResults(
					"data_source_list_" + opCount.getAndIncrement(),
					URI.create(config.getManagerUri()), null, null,
					ImmutableList.of(), ImmutableList.of(), stats, null, null,
					0L);
			ResponseBuilder response = Response.ok(queryResults);
			return response.build();
		}
	}

	public static String buildDsResult(DataSource ds) {
		StringBuilder sb = new StringBuilder();
		sb.append("type:" + ds.getType());
		sb.append("\n");
		sb.append("name:" + ds.getName());
		sb.append("\n");
		sb.append("nodes:" + ds.getNodes());
		sb.append("\n");
		sb.append("allowConcurrentPerNode:" + ds.isAllowConcurrentPerNode());
		sb.append("\n");
		sb.append("className:" + ds.getClassName());
		sb.append("\n");
		sb.append("properties:[");
		Iterator<Entry<String, Object>> it = ds.getProperties().entrySet()
				.iterator();
		boolean first = true;
		while (it.hasNext()) {
			if (!first) {
				sb.append("\n");
			} else {
				first = false;
			}
			Entry<String, Object> en = it.next();
			sb.append(en.getKey() + ":" + en.getValue());
		}
		return sb.toString();
	}

	public Response deleteDatasource(String name) {
		HttpUriBuilder uriBuilder = uriBuilderFrom(
				URI.create(config.getManagerUri()));
		uriBuilder.appendPath("/v1/datasource/delete/" + name);
		Request request = prepareDelete().setUri(uriBuilder.build())
				.setHeader(HttpHeaders.CONTENT_TYPE,
						MediaType.JSON_UTF_8.toString())
				.build();
		StatusResponse res = httpClient.execute(request,
				createStatusResponseHandler());
		if (res.getStatusCode() != Status.OK.getStatusCode()) {
			StatementStats stats = new StatementStats("FAILED", false, false, 0,
					0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, null);
			QueryError error = new QueryError(res.getStatusMessage(), null,
					res.getStatusCode(), "DELETE_HTTP_ERROR",
					res.getStatusCode() + "", null, null);
			QueryResults queryResults = new QueryResults(
					"data_source_delete_" + opCount.getAndIncrement(),
					URI.create(config.getManagerUri()), null, null, null, null,
					stats, error, "DELETE DATASOURCE", null);
			ResponseBuilder response = Response.ok(queryResults);
			return response.build();
		} else {
			StatementStats stats = new StatementStats("FINISHED", false, false,
					0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, null);
			Column column = new Column("result", "boolean",
					new ClientTypeSignature(StandardTypes.BOOLEAN,
							ImmutableList.of()));
			List<Object> data = ImmutableList.of(true);
			QueryResults queryResults = new QueryResults(
					"data_source_delete_" + opCount.getAndIncrement(),
					URI.create(config.getManagerUri()), null, null,
					ImmutableList.of(column), ImmutableList.of(data), stats,
					null, "DELETE DATASOURCE", null);
			ResponseBuilder response = Response.ok(queryResults);
			return response.build();
		}
	}
}
