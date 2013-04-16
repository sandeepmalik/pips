package com.open.qbes.conf;

import com.open.qbes.queues.Queue;

/**
 * User: smalik
 * Date: 7/18/12
 * Time: 4:05 PM
 */
public interface QueueConfig {

    String getQueueId();

    Queue getAssociatedQueue();

    String type();

    String mode();

    String cacheType();

    String jobType();

    <V> V get(String key, V defaultVal);

    void set(String key, Object value);

    boolean has(String key);

    void update(QueueConfig newConfig);

    void setOneTimeAssociatedQueue(Queue queue);
}
