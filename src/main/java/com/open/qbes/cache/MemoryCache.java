package com.open.qbes.cache;

import com.open.qbes.conf.QueueConfig;
import com.open.qbes.core.Job;
import com.open.utils.Log;
import com.open.utils.ThreadSafe;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@ThreadSafe(true)
public class MemoryCache<J extends Job> implements JobCache<J> {

    private static final Log log = Log.getLogger(MemoryCache.class);

    private BlockingQueue<J> memoryStore;

    @Override
    public <T> void init(QueueConfig queueConfig) {
        this.memoryStore = new LinkedBlockingQueue<>(queueConfig.get("capacity", Integer.MAX_VALUE));
    }

    @Override
    public boolean isEmpty() {
        return memoryStore.isEmpty();
    }

    @Override
    public int getInitialJobCount() {
        return 0;
    }

    @Override
    public void suspend() {
        // TODO: should try and store the data to some persistent store:
        throw new UnsupportedOperationException();
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void put(J job) throws Exception {
        memoryStore.put(job);
    }

    @Override
    public J get() throws Exception {
        J job = memoryStore.take();
        return job;
    }

    @Override
    public void shutdown() {
        log.info("Shutting down");
        this.memoryStore = null;
    }


}
