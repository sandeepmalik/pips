package com.open.qbes.api.http;

import com.open.qbes.conf.QueueConfig;
import com.open.qbes.conf.QueueConfigMap;
import com.open.qbes.core.Job;
import com.open.qbes.core.JobContext;
import com.open.qbes.core.QueueService;
import com.open.qbes.core.annotations.ContextConfiguration;
import com.open.qbes.queues.QueueStats;
import com.open.utils.Log;
import com.open.utils.Pair;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Future;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path(Constants.API_VERSION + "/queue/{queue_id}")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@ContextConfiguration
public class QueueHTTPRequestHandler {

    private static final Log log = Log.getLogger(QBESHTTPRequestHandler.class);

    @POST
    @Path("/enqueue")
    public Response enqueue(@PathParam("queue_id") String queueId, Map<String, Object> requestJSON) throws Exception {
        Job<Object> job = QueueService.getInstance().createJob(QueueService.getInstance().getQueueConfig(queueId), requestJSON);
        Object result = JobContext.getContext().enqueue(job);
        if (!(result instanceof Future)) {
            return Constants.Utils.response(Pair.pair("result", result), Pair.pair(Constants.STATUS_KEY, "FINISHED"));
        } else {
            return Constants.Utils.response(Pair.pair(Constants.STATUS_KEY, "ENQUEUED"), Pair.pair("enqueued_at", new Date().toString()));
        }
    }

    @POST
    @Path("/update")
    public Response update(@PathParam("queue_id") String queueId, Map<String, Object> requestJSON) throws IOException {
        log.debug("Updating queue with id " + queueId);
        requestJSON.put("queueId", queueId);
        QueueConfig config = QueueConfigMap.buildFrom(requestJSON);
        QueueStats queueStats = QueueService.getInstance().update(config).stats();
        return Response.ok().entity(queueStats).build();
    }

    @POST
    @Path("/shutdown")
    public Response suspend(@PathParam("queue_id") String queueId) throws IOException {
        log.debug("Suspending queue with id " + queueId);
        QueueStats queueStats = QueueService.getInstance().suspend(queueId);
        return Response.ok().entity(queueStats).build();
    }

    @POST
    @Path("/delete")
    public Response delete(@PathParam("queue_id") String queueId) throws IOException {
        QueueStats stats = QueueService.getInstance().delete(queueId);
        log.debug("Deleting queue with id " + queueId);
        return Response.ok().entity(stats).build();
    }

    @GET
    public Response status(@PathParam("queue_id") String queueId) throws IOException {
        log.debug("Sending stats for queue with id " + queueId);
        QueueStats queueStats = QueueService.getInstance().stats(queueId);
        return Response.ok().entity(queueStats).build();
    }

    @POST
    @Path("/create")
    public Response create(@PathParam("queue_id") String queueId, Map<String, Object> requestJSON) throws IOException {
        if (requestJSON == null)
            throw new IllegalArgumentException("No Queue Config data");
        requestJSON.put("queueId", queueId);
        log.debug("Creating a new queue with id " + queueId);
        QueueConfig config = QueueConfigMap.buildFrom(requestJSON);
        QueueStats stats = QueueService.getInstance().create(config).stats();
        return Response.ok().entity(stats).build();
    }

}
