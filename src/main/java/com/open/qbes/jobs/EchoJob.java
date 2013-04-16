package com.open.qbes.jobs;

import com.open.qbes.core.AbstractJob;
import com.open.qbes.core.annotations.EnqueueRootElement;
import com.open.utils.Log;
import com.open.qbes.core.JobContext;

@EnqueueRootElement("echo")
public class EchoJob<T> extends AbstractJob<T> {

    private static Log log = Log.getLogger("Echo");

    @Override
    public T doCall() throws Exception {
        log.debug(JobContext.getContext().getInitialJobData().toString());
        return null;
    }

    @Override
    public String toString() {
        return "EchoJob(" + JobContext.getContext().getInitialJobData() + ")";
    }
}
