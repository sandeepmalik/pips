package com.open.qbes.core.exceptions;

import com.open.qbes.api.http.StatusCode;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 2/8/13
 * Time: 5:41 PM
 */
public class APIException extends RuntimeException {

    private final StatusCode statusCode;

    public APIException(StatusCode statusCode) {
        this.statusCode = statusCode;
    }

    public APIException(StatusCode statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }

    public APIException(StatusCode statusCode, String message, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public APIException(StatusCode statusCode, Throwable cause) {
        super(cause);
        this.statusCode = statusCode;
    }

    public APIException(StatusCode statusCode, String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.statusCode = statusCode;
    }

    public StatusCode getStatusCode() {
        return statusCode;
    }
}
