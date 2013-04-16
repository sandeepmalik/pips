package com.open.qbes.core;

import com.open.qbes.api.http.StatusCode;

/**
 * Created by smalik
 * User: Sandeep Malik
 * Date: 1/16/13
 * Time: 9:54 PM
 */
public interface ErrorCollector {

    int errorCount();

    void setStatusCode(StatusCode statusCode);

    StatusCode getStatusCode();

    boolean suppressErrors();

    void reportException(Throwable t);

    void reportException(String message, Throwable t);

    String getErrorsReport();
}
