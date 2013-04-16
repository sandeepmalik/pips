package com.open.qbes.core;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 8/23/12
 * Time: 6:31 PM
 */
public interface JobCallback<T> {

    public void callback(Job<T> finishedJob, Object result, boolean isException);

}
