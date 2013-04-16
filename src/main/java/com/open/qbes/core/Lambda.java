package com.open.qbes.core;

import com.open.utils.Log;
import com.open.utils.StringUtils;

public abstract class Lambda<T> extends AbstractJob<T> {

    private static final Log log = Log.getLogger(Lambda.class);

    @Override
    protected final T doCall() throws Exception {
        log.debug("Executing task %s", StringUtils.asString(this));
        return execute();
    }

    protected abstract T execute() throws Exception;
}
