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
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.alibaba.fastjson.JSON;
import com.facebook.presto.lucene.base.DataSource;
import com.facebook.presto.lucene.base.HeartBeatCommand;
import com.facebook.presto.lucene.base.HostDataSources;

import io.airlift.log.Logger;

@Path("/v1/datasource")
public class DataSourceResource {

    private static final Logger log = Logger.get(DataSourceResource.class);

    private final WeiwoDataSourceManager sourceManager;

    @Inject
    public DataSourceResource(WeiwoDataSourceManager sourceManager) {
        this.sourceManager = requireNonNull(sourceManager, "sourceManager is null");
    }

    @POST
    @Path("/add")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response add(DataSource source) {
        
        requireNonNull(source, "source is null");

        log.info("Open writer info. " + JSON.toJSONString(source));

        try {
            sourceManager.addDataSource(source);
            return Response.ok().build();
        } catch (IOException e) {
            log.error(e, "add error.");
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }

    }

    @DELETE
    @Path("/delete/{sourceName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response delete(@PathParam("sourceName") String sourceName) {
        boolean success = false;
        try {
            success = sourceManager.deleteDataSource(sourceName);
        } catch (IOException e) {
            log.error(e, "delete error.");
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }
        if (!success) {
            return Response.status(Status.BAD_REQUEST).build();
        }
        return Response.status(Status.OK).build();
    }

    @GET
    @Path("/list")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<DataSource> list() {
        return sourceManager.listDataSource();
    }

    @GET
    @Path("/get/{sourceName}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public DataSource get(@PathParam("sourceName") String sourceName) throws IOException {
        return sourceManager.getDataSource(sourceName);
    }
    
    @POST
    @Path("/heartbeat")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HeartBeatCommand heartbeat(HostDataSources hostDataSources) {
        return sourceManager.heartBeat(hostDataSources);
    }

}
