package com.open.qbes.cache;

import com.open.qbes.cache.mq.EDBPersistentQueue;
import com.open.qbes.conf.QueueConfig;
import com.open.qbes.core.Job;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 7/23/12
 * Time: 10:40 AM
 */
public class CacheInitializer {
    private static CacheInitializer ourInstance = new CacheInitializer();

    public static CacheInitializer getInstance() {
        return ourInstance;
    }

    private CacheInitializer() {
    }

    @SuppressWarnings("unchecked")
    public <J extends Job> JobCache<J> getJobCache(QueueConfig queueConfig) {
        String type = queueConfig.cacheType();
        JobCache<J> cache;
        if (type == null)
            throw new NullPointerException("no cache type specified");
        if ("memory".equalsIgnoreCase(type))
            cache = new MemoryCache<>();
        else if ("db".equalsIgnoreCase(type)) {
            // check for other properties
            cache = new EDBPersistentQueue();
        } else throw new IllegalArgumentException("unknown cache type: " + type);
        cache.init(queueConfig);
        return cache;
    }
}
