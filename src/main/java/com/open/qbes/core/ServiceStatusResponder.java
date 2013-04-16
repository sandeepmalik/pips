package com.open.qbes.core;

import com.open.qbes.api.http.StatusCode;
import com.open.utils.Pair;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 2/4/13
 * Time: 1:46 PM
 */
public interface ServiceStatusResponder {

    ServiceStatus getStatus();

    public static interface ServiceStatus {

        String checkedAt();

        String getServiceName();

        Pair<StatusCode, String> getStatus();
    }
}
