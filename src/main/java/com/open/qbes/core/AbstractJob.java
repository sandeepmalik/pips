package com.open.qbes.core;

import com.open.utils.ExceptionUtils;
import com.open.utils.Log;

import java.util.Map;
import java.util.UUID;

public abstract class AbstractJob<T> implements Job<T>, CallbackCompletionService<T> {

    private static final Log log = Log.getLogger(AbstractJob.class);

    protected int priority;

    private volatile JobCallback callback;

    private final String id = UUID.randomUUID().toString();

    @Override
    public void validate(Map<String, Object> jobData) {
    }

    public AbstractJob() {
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    @SuppressWarnings("unchecked")
    public final T call() throws Exception {
        try {
            T result = doCall();
            if (callback != null) {
                callback.callback(this, result, false);
            }
            return result;
        } catch (Throwable e) {
            log.error("Error in job execution %s", e, ExceptionUtils.getExceptionString(e).replace("\n", "..."));
            if (callback != null) {
                callback.callback(this, e, true);
            }
            throw e;
        }
    }

    protected abstract T doCall() throws Exception;

    @Override
    public int priority() {
        return priority;
    }

    @Override
    public int compareTo(Job<T> o) {
        return new Integer(this.priority()).compareTo(o.priority());
    }

    @Override
    public void onComplete(JobCallback callbackJob) {
        callback = callbackJob;
    }

    @Override
    public void onCallbackComplete(JobCallback<T> jobCallback) {
        if (jobCallback != null)
            log.debug("%s callback complete", jobCallback.getClass().getSimpleName());
    }
}
