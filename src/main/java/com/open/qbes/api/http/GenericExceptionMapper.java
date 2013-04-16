package com.open.qbes.api.http;

import com.open.qbes.core.JobContext;
import com.open.qbes.core.exceptions.APIException;
import com.open.utils.Log;
import com.open.utils.ExceptionUtils;
import com.open.utils.Pair;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import static com.open.utils.JSONUtils.map;
import static javax.ws.rs.core.Response.status;

@Provider
public class GenericExceptionMapper implements ExceptionMapper<Throwable> {

    private static final Log log = Log.getLogger(GenericExceptionMapper.class);

    @Override
    public Response toResponse(Throwable exception) {
        Response.Status status = Response.Status.INTERNAL_SERVER_ERROR;
        if (exception instanceof APIException) {
            APIException APIException = (APIException) exception;
            status = Response.Status.fromStatusCode(APIException.getStatusCode().getCode());
        }
        if (exception instanceof WebApplicationException) {
            WebApplicationException webApplicationException = (WebApplicationException) exception;
            status = Response.Status.fromStatusCode(webApplicationException.getResponse().getStatus());
            if (status == null) {
                if (webApplicationException.getResponse().getStatus() == 405)
                    status = Response.Status.NOT_FOUND;
                else status = Response.Status.INTERNAL_SERVER_ERROR;
            }
        }
        String errorReport = JobContext.concatenateErrors(ExceptionUtils.getExceptionString(exception));
        log.error("Error in serving response: %s", exception, errorReport);
        return status(status.getStatusCode()).entity(
                map(
                        Pair.pair("status_code", status.getStatusCode()),
                        Pair.pair("status", status.name())
                )
        ).build();
    }
}
