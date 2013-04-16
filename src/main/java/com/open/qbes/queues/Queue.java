package com.open.qbes.queues;

import com.open.qbes.conf.QueueConfig;
import com.open.qbes.core.Job;

import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * User: smalik
 * Date: 7/18/12
 * Time: 4:03 PM
 */
public interface Queue {

    void suspend();

    void start(QueueConfig queueConfig);

    QueueStats destroy();

    QueueStats stats();

    void update(QueueConfig config);

    boolean isActive();

    Executor getAssociatedExecutor();

    <T> Future<T> enqueue(Job<T> job);

    CompletionService<?> enqueue(Job... jobs);
}
