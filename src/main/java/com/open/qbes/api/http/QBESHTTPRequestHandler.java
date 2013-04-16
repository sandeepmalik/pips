package com.open.qbes.api.http;

import com.open.qbes.QBES;
import com.open.qbes.core.QueueService;
import com.open.qbes.core.ServiceStatusResponder;
import com.open.qbes.core.annotations.ContextConfiguration;
import com.open.utils.ExceptionUtils;
import com.open.utils.Log;
import com.open.utils.JSONUtils;
import com.open.utils.Pair;
import com.startup.StartupOptions;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.open.utils.JSONUtils.map;
import static com.open.utils.ThreadDump.getThreadDump;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.ok;

@Path("/")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@ContextConfiguration
public class QBESHTTPRequestHandler {

    private static final Log log = Log.getLogger(QBESHTTPRequestHandler.class);

    @GET
    @Path("/ping")
    public Response ping() {
        try {
            log.debug("Ping requested");
            return Constants.Utils.response(Pair.pair(Constants.STATUS_KEY, "ALIVE"));
        } catch (Exception e) {
            System.out.println("here");
            throw e;
        }
    }

    @GET
    @Path("/status")
    public Response status() {
        log.debug("Overall status requested");

        String sha = "Unknown";
        String branch = "Unknown";
        String build = "Unknown";

        try {
            InputStream is = this.getClass().getResourceAsStream("/version-info.properties");
            if (is != null) {
                Properties properties = new Properties();
                properties.load(is);
                if (properties.getProperty("commit") != null)
                    sha = properties.getProperty("commit");
                if (properties.getProperty("branch") != null)
                    branch = properties.getProperty("branch");
                if (properties.getProperty("build") != null)
                    build = properties.getProperty("build");

            }
        } catch (Exception ignore) {
        }

        final String failing = "failing";
        final String ok = "ok";
        try {
            List<ServiceStatusResponder.ServiceStatus> statusList = QueueService.getInstance().getServiceStatus();
            List<Map<String, Object>> dependencies = new ArrayList<>();

            boolean isAllOk = true;
            for (ServiceStatusResponder.ServiceStatus serviceStatus : statusList) {
                dependencies.add(
                        JSONUtils.map(
                                Pair.pair(serviceStatus.getServiceName(),
                                        JSONUtils.map(
                                                Pair.pair("status", serviceStatus.getStatus().getItem1() == StatusCode.OK ? ok : failing),
                                                Pair.pair("code", String.valueOf(serviceStatus.getStatus().getItem1().getCode())),
                                                Pair.pair("name", serviceStatus.getServiceName()),
                                                Pair.pair("checked_at", serviceStatus.checkedAt()),
                                                Pair.pair("message", serviceStatus.getStatus().getItem1() == StatusCode.OK ? "" : serviceStatus.getStatus().getItem2())
                                        )
                                )
                        )
                );
                isAllOk = serviceStatus.getStatus().getItem1() == StatusCode.OK;
            }
            Map m = JSONUtils.map(
                    Pair.pair("status", isAllOk ? ok : failing),
                    Pair.pair("name", StartupOptions.context),
                    Pair.pair("code", String.valueOf(isAllOk ? StatusCode.OK.getCode() : StatusCode.SERVICE_UNAVAILABLE.getCode())),
                    Pair.pair("checked_at", new SimpleDateFormat("yyyy/MM/dd hh:mm:ss").format(new Date())),
                    Pair.pair("dependencies", dependencies),
                    Pair.pair("SHA", sha),
                    Pair.pair("BRANCH", branch),
                    Pair.pair("BUILD_ID", build)
            );
            if (isAllOk) {
                return Response.ok().entity(m).build();
            } else {
                return Response.status(StatusCode.SERVICE_UNAVAILABLE.getCode()).entity(m).build();
            }
        } catch (Throwable e) {
            log.error("Error in requesting overall status", e);
            return Constants.Utils.response(
                    Pair.pair("status", failing),
                    Pair.pair("code", StatusCode.INTERNAL_SERVER_ERROR.getCode()),
                    Pair.pair("checked_at", new SimpleDateFormat("yyyy/MM/dd hh:mm:ss").format(new Date())),
                    Pair.pair("message", ExceptionUtils.getExceptionString(e)),
                    Pair.pair("SHA", sha),
                    Pair.pair("BRANCH", branch),
                    Pair.pair("BUILD_ID", build)
            );
        }
    }

    @GET
    @Path("/shutdown")
    public Response shutdown() {
        log.debug("Shutting down the QBES");
        // shut down the queue service:
        log.info("Shutting down the QueueService");
        QueueService.getInstance().shutdown();
        log.info("QBES shutdown!");
        return Constants.Utils.response(Pair.pair(Constants.STATUS_KEY, "SHUTDOWN"));
    }

    @GET
    @Path("/halt")
    public Response halt() {
        log.debug("Halting the queue system");
        return Constants.Utils.response(Pair.pair(Constants.STATUS_KEY, QueueService.getInstance().halt()));
    }

    @GET
    @Path("/jobs/reload")
    public Response reloadMappings() throws Exception {
        log.debug("Reloading job mappings ");
        QueueService.getInstance().reloadMappings();
        return Constants.Utils.response(Pair.pair(Constants.STATUS_KEY, "Reloaded Mappings"));
    }

    @GET
    @Path("/logger")
    public Response handleLoggerOperation(@DefaultValue("rootLogger") @QueryParam("logger") String logger, @DefaultValue("DEBUG") @QueryParam("level") String level) {
        log.debug("Handling logger operations");
        Logger.getLogger(logger).setLevel(Level.toLevel(level.toUpperCase()));
        log.info("Logger changed. Logger %s, to new level %s", logger, level);
        return Constants.Utils.response(Pair.pair(Constants.STATUS_KEY, "Log Levels Changed"));
    }

    @GET
    @Path("/$t_dump$")
    public Response threadDump() {
        log.debug("Returning thread dump");
        return ok(getThreadDump(false)).build();
    }

    @GET
    @Path("/$t_dump_print$")
    public Response printThreadDump() {
        log.debug("Returning thread dump");
        getThreadDump(true);
        return ok(getThreadDump(false)).build();
    }

    @GET
    @Path("/is_started")
    public Response getSystemStats(@QueryParam("wait_for_start") boolean waitForStart, @QueryParam("timeout") long timeout) {
        if (!waitForStart) {
            return Constants.Utils.response(Pair.pair("started", QBES.isStarted()));
        } else {
            timeout = timeout == 0 ? 3600 : Math.min(Math.abs(timeout), 3600);
            try {
                QBES.waitForSystemStart(timeout);
                return Constants.Utils.response(Pair.pair("started", true));
            } catch (InterruptedException e) {
                return Constants.Utils.response(Pair.pair("started", false), Pair.pair("timed_out", true));
            }

        }
    }
}