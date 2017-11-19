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
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import com.facebook.presto.lucene.base.IndexInfoWithNodeInfoSimple;

import io.airlift.log.Logger;

@Path("/v1/index")
public class LuceneIndexResource {

    private static final Logger log = Logger.get(LuceneIndexResource.class);

    private final LuceneIndexManager indexManager;
    private final LuceneIndexWriterManager indexWriterManager;
    private final NodeStateChangeService stateChangeService;

    @Inject
    public LuceneIndexResource(LuceneIndexManager indexManager, LuceneIndexWriterManager indexWriterManager,
            NodeStateChangeService stateChangeService) {
        this.indexManager = requireNonNull(indexManager, "indexManager is null");
        this.indexWriterManager = requireNonNull(indexWriterManager, "indexWriterManager is null");
        this.stateChangeService = requireNonNull(stateChangeService, "stateChangeService is null");
    }

    @POST
    @Path("/clear")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response clear(String nodeId) {
        requireNonNull(nodeId, "nodeId is null");

        log.info("Clear info. node = " + nodeId);

        stateChangeService.clear(nodeId);

        return Response.ok().build();
    }

    @POST
    @Path("/writer")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response writer(IndexInfoWithNodeInfoSimple indexInfoWithNode) {
        requireNonNull(indexInfoWithNode, "indexInfoWithNode is null");

        if (indexInfoWithNode.getNodes().size() != 1) {
            log.error("Open writer nodes num not correct." + indexInfoWithNode.getNodes().size());
            return Response.status(Status.BAD_REQUEST).build();
        }

        log.info("Open writer info. " + indexInfoWithNode);

        indexWriterManager
                .open(new IndexInfoWithNodeInfo(indexInfoWithNode.getIndexInfo(), indexInfoWithNode.getNodes()));

        stateChangeService.addObserver(indexInfoWithNode.getNodes().get(0).getNodeId(), new WriterObserver(
                indexWriterManager, indexInfoWithNode.getIndexInfo(), indexInfoWithNode.getNodes().get(0)));

        return Response.ok().build();
    }

    @POST
    @Path("/writer/close")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response writerClose(IndexInfoWithNodeInfoSimple indexInfoWithNode) {
        requireNonNull(indexInfoWithNode, "indexInfoWithNode is null");

        if (indexInfoWithNode.getNodes().size() != 1) {
            log.error("Close writer nodes num not correct." + indexInfoWithNode.getNodes().size());
            return Response.status(Status.BAD_REQUEST).build();
        }

        log.info("Close writer info. " + indexInfoWithNode);

        indexWriterManager
                .close(new IndexInfoWithNodeInfo(indexInfoWithNode.getIndexInfo(), indexInfoWithNode.getNodes()));

        stateChangeService.deleteObserver(indexInfoWithNode.getNodes().get(0).getNodeId(), new WriterObserver(
                indexWriterManager, indexInfoWithNode.getIndexInfo(), indexInfoWithNode.getNodes().get(0)));

        return Response.ok().build();
    }

    @GET
    @Path("/list/writer")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<IndexInfoWithNodeInfoSimple> getIndexWriterList(@QueryParam("db") String db,
            @QueryParam("table") String table, @QueryParam("partitions") List<String> partitions,
            @Context UriInfo uriInfo) {
        requireNonNull(db, "db is null");
        requireNonNull(partitions, "partitions is null");
        requireNonNull(table, "table is null");
        List<IndexInfoWithNodeInfo> list = indexWriterManager.getIndexList(db, table, partitions);
        List<IndexInfoWithNodeInfoSimple> result = new ArrayList<>();
        if (list != null) {
            list.forEach(info -> result.add(new IndexInfoWithNodeInfoSimple(info.getIndexInfo(), info.getNodes())));
        }
        return result;
    }

    @POST
    @Path("/open")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response open(IndexInfoWithNodeInfoSimple indexInfoWithNode) {
        requireNonNull(indexInfoWithNode, "indexInfoWithNode is null");

        if (indexInfoWithNode.getNodes().size() != 1) {
            log.error("Open index nodes num not correct." + indexInfoWithNode.getNodes().size());
            return Response.status(Status.BAD_REQUEST).build();
        }

        log.info("Open index info. " + indexInfoWithNode);

        indexManager.open(new IndexInfoWithNodeInfo(indexInfoWithNode.getIndexInfo(), indexInfoWithNode.getNodes()));

        stateChangeService.addObserver(indexInfoWithNode.getNodes().get(0).getNodeId(),
                new IndexObserver(indexManager, indexInfoWithNode.getIndexInfo(), indexInfoWithNode.getNodes().get(0)));

        return Response.ok().build();
    }

    @POST
    @Path("/close")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response close(IndexInfoWithNodeInfoSimple indexInfoWithNode) {
        requireNonNull(indexInfoWithNode, "indexInfoWithNode is null");

        if (indexInfoWithNode.getNodes().size() != 1) {
            log.error("Close index nodes num not correct." + indexInfoWithNode.getNodes().size());
            return Response.status(Status.BAD_REQUEST).build();
        }

        log.info("Close index info. " + indexInfoWithNode);

        indexManager.close(new IndexInfoWithNodeInfo(indexInfoWithNode.getIndexInfo(), indexInfoWithNode.getNodes()));

        stateChangeService.deleteObserver(indexInfoWithNode.getNodes().get(0).getNodeId(),
                new IndexObserver(indexManager, indexInfoWithNode.getIndexInfo(), indexInfoWithNode.getNodes().get(0)));

        return Response.ok().build();
    }

    @GET
    @Path("/list")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<IndexInfoWithNodeInfoSimple> getIndexList(@QueryParam("db") String db,
            @QueryParam("table") String table, @QueryParam("partitions") List<String> partitions,
            @Context UriInfo uriInfo) {
        requireNonNull(db, "db is null");
        requireNonNull(partitions, "partitions is null");
        requireNonNull(table, "table is null");
        List<IndexInfoWithNodeInfo> list = indexManager.getIndexList(db, table, partitions);
        if (list != null) {
            return list.stream().map(info -> new IndexInfoWithNodeInfoSimple(info.getIndexInfo(), info.getNodes()))
                    .collect(Collectors.toList());
        } else {
            return new ArrayList<>();
        }
    }

}
