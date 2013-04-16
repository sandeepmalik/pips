package com.open.qbes.cache;

import com.open.qbes.conf.QueueConfig;
import com.open.qbes.core.Job;

public interface JobCache<J extends Job> {

    <T> void init(QueueConfig queueConfig);

    boolean isEmpty();

    int getInitialJobCount();

    void suspend();

    int remainingCapacity();

    void put(J job) throws Exception;

    J get() throws Exception;

    void shutdown();
}
