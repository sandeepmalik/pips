package com.open.qbes.api.http;

import com.open.qbes.core.annotations.ContextConfiguration;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.Map;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/queue/{queue_id}")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@Deprecated
@ContextConfiguration
public class BackwardCompatibleQueueHTTPRequestHandler {

    @POST
    @Path("/enqueue")
    public Response enqueue(@PathParam("queue_id") String queueId, Map<String, Object> requestJSON) throws Exception {
        return new QueueHTTPRequestHandler().enqueue(queueId, requestJSON);
    }

}
