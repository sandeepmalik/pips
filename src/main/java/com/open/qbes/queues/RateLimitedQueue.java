package com.open.qbes.queues;

import com.open.qbes.cache.CacheInitializer;
import com.open.qbes.cache.JobCache;
import com.open.qbes.conf.QueueConfig;
import com.open.qbes.core.Job;
import com.open.utils.JSONUtils;
import com.open.utils.Log;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RateLimitedQueue implements Queue {

    private static final Log log = Log.getLogger(RateLimitedQueue.class);

    private final Queue delegatedQueue;
    private JobCache cache;
    private ScheduledThreadPoolExecutor rateLimitedExecutor;
    private ScheduledFuture<?> scheduledFuture;
    private QueueConfig config;
    private volatile long startTime = -1;
    private AtomicLong processCounter = new AtomicLong();

    public RateLimitedQueue(Queue delegatedQueue) {
        this.delegatedQueue = delegatedQueue;
    }

    @Override
    public void suspend() {
        log.info("Suspending " + config.getQueueId());
        log.debug("Cancelling the scheduled jobs for " + config.getQueueId());
        scheduledFuture.cancel(true);
        rateLimitedExecutor.shutdown();
        log.info("Suspending the executor");
        delegatedQueue.suspend();
        log.debug("Suspending cache for " + config.getQueueId());
        cache.suspend();
    }

    public void start(QueueConfig queueConfig) {
        this.config = queueConfig;
        log.debug(config.getQueueId() + " starting");
        log.trace(config.getQueueId() + " starting the delegated queue");
        delegatedQueue.start(queueConfig);

        log.debug("Starting the cache for " + config.getQueueId());
        cache = CacheInitializer.getInstance().getJobCache(queueConfig);
        log.info("Created cache for " + config.getQueueId() + ", " + cache);

        long delay = calculateDelay(queueConfig);

        log.debug("Starting timer for " + config.getQueueId());
        rateLimitedExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "RLQTimer[" + config.getQueueId() + "]");
            }
        });
        rateLimitedExecutor.setRemoveOnCancelPolicy(true);
        rateLimitedExecutor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        rateLimitedExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

        log.info("Execution for " + queueConfig.getQueueId() + " scheduled at delay of " + delay + " ms/job");

        scheduledFuture = rateLimitedExecutor.scheduleAtFixedRate(new ScheduledTask<>(this), 0, delay, MILLISECONDS);
    }

    private static class ScheduledTask<T, J extends Job<T>> implements Runnable {
        private RateLimitedQueue queue;

        private ScheduledTask(RateLimitedQueue queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                log.trace("Trying to schedule the next job");
                Job job = queue.cache.get();
                if (job != null) {
                    if (queue.startTime == -1) {
                        queue.startTime = new Date().getTime();
                    }
                    log.debug("Scheduling next job");
                    queue.delegatedQueue.enqueue(job);
                    queue.processCounter.incrementAndGet();
                }
            } catch (Exception e) {
                if (e instanceof InterruptedException && queue.rateLimitedExecutor.isShutdown()) {
                    log.warn("Scheduled job failed because queue was shutdown");
                } else log.error("Failed scheduled job %s", e, e.getMessage());
            }
        }
    }

    private long calculateDelay(QueueConfig queueConfig) {
        log.debug("Recalculating delay");
        double throughput_per_day = JSONUtils.doubleToInt("throughput_per_day", (Map<String, Object>) queueConfig, -1);

        if (throughput_per_day < 1) {
            throw new IllegalArgumentException("no throughput_per_day specified");
        }

        long delay = (long) (24 * 3600 * 1000 / throughput_per_day);

        if (delay < 100) {
            log.warn("The throughput is too high to realize: " + throughput_per_day + " for " + config.getQueueId());
        }

        queueConfig.set("minimal_interval", delay);
        return delay;
    }

    @Override
    public QueueStats destroy() {
        log.info("Destroying " + config.getQueueId());
        log.debug("Cancelling the scheduled jobs for " + config.getQueueId());
        scheduledFuture.cancel(true);
        rateLimitedExecutor.shutdown();
        log.debug("Timer shutdown for " + config.getQueueId());
        cache.shutdown();
        log.debug("Cache shutdown for " + config.getQueueId());
        log.debug("Destroying delegated queue for " + config.getQueueId());
        return delegatedQueue.destroy();
    }

    @Override
    public QueueStats stats() {
        QueueStats queueStats = delegatedQueue.stats();
        if (startTime != -1) {
            long diff = new Date().getTime() - startTime;
            if (diff > 0)
                queueStats.setThroughput(processCounter.get() * 1000 * 60 * 60 * 24 / (new Date().getTime() - startTime));
        }
        queueStats.setJobsProcessed(queueStats.getJobsProcessed() + cache.getInitialJobCount());
        return queueStats;
    }

    @Override
    public synchronized void update(QueueConfig tQueueConfig) {
        if (scheduledFuture != null) {
            log.info("Performing update for " + config.getQueueId());
            log.debug("Cancelling scheduled jobs for " + config.getQueueId());
            scheduledFuture.cancel(false);
            long delay = calculateDelay(tQueueConfig);
            log.info("Rescheduling at the new delay rate of " + delay + " ms/job for " + config.getQueueId());
            scheduledFuture = rateLimitedExecutor.scheduleAtFixedRate(new ScheduledTask<>(this), 0, delay, MILLISECONDS);
            config.update(tQueueConfig);
            log.debug("Config updated for " + config.getQueueId());
            log.debug("Updating the delegated queue");
            delegatedQueue.update(config);
        }
    }

    @Override
    public boolean isActive() {
        return delegatedQueue.isActive();
    }

    @Override
    public Executor getAssociatedExecutor() {
        return delegatedQueue.getAssociatedExecutor();
    }

    public Future enqueue(Job job) {
        if (cache.remainingCapacity() == 0) {
            throw new IllegalStateException("Queue Full");
        } else {
            try {
                cache.put(job);
            } catch (Exception e) {
                throw new IllegalStateException("Queue Full");
            }
        }
        return null;
    }

    @Override
    public CompletionService<?> enqueue(Job... jobs) {
        throw new UnsupportedOperationException();
    }
}
