package com.open.qbes.queues;

import com.open.qbes.conf.QueueConfig;
import com.open.qbes.core.AbstractJob;
import com.open.qbes.core.ExecutionContext;
import com.open.qbes.core.Job;
import com.open.utils.JSONUtils;
import com.open.utils.Log;
import com.open.utils.ThreadSafe;
import com.open.qbes.core.JobContext;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe(true)
public class MemoryBasedQueue implements Queue {

    private static final Log log = Log.getLogger(MemoryBasedQueue.class);

    private BlockingQueue<Runnable> queue;
    private volatile boolean isRunning;
    private AtomicInteger serial = new AtomicInteger();
    private ThreadPoolExecutor executor;
    private QueueConfig queueConfig;

    @Override
    public void suspend() {
        isRunning = false;
        executor.shutdown();
        // let the currently executing tasks finish, for at least 1 minute:
        try {
            log.info("Executor shutdown for " + queueConfig.getQueueId() + ", waiting for currently executing jobs to finish");
            if (executor.awaitTermination(1, MINUTES)) {
                log.info("Finished up all the currently executing jobs for " + queueConfig.getQueueId());
            } else {
                log.info("Could not finish all the currently executing jobs even after 1 minute, some jobs may have been lost.");
            }
        } catch (InterruptedException e) {
            log.error("Could not process currently executing jobs after shutdown request. Some jobs may have been lost.");
        }
    }

    @Override
    public void start(QueueConfig queueConfig) {
        this.queueConfig = queueConfig;
        log.debug(name() + " starting");
        isRunning = true;
        queue = new SynchronousQueue<Runnable>() {
            @Override
            public Runnable take() throws InterruptedException {
                if (!isRunning) {
                    throw new IllegalStateException("Queue Stopped");
                } else return super.take();
            }

            @Override
            public void put(Runnable e) throws InterruptedException {
                if (!isRunning) {
                    throw new IllegalStateException("Queue Stopped");
                } else super.put(e);
            }
        };
        log.debug("Queue Created, Initializing the executor");
        ThreadFactory tf = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "<" + name() + ">::Worker[" + serial.incrementAndGet() + "]");
            }
        };

        if (queueConfig.has("mode") && "concurrent".equals(queueConfig.mode())) {
            Map<String, Object> configData = (Map<String, Object>) queueConfig;
            int defaultCoreSize = 2 * Runtime.getRuntime().availableProcessors();
            // use at least 5 core threads:
            defaultCoreSize = Math.max(5, defaultCoreSize);
            int maxCoreSize = Integer.MAX_VALUE;
            executor = new MBQThreadPoolExecutor(JSONUtils.doubleToInt("core_size", configData, defaultCoreSize), JSONUtils.doubleToInt("max_size", configData, maxCoreSize), JSONUtils.doubleToInt("wait_time", configData, 60), SECONDS, queue, tf);
        } else {
            executor = new MBQThreadPoolExecutor(1, 1, 0, SECONDS, queue, tf);
        }
        log.debug("Configured executor with " + executor.getCorePoolSize() + " as core pool size, " + executor.getMaximumPoolSize() + " as max pool size, " + executor.getKeepAliveTime(SECONDS) + " as wait time (secs)");
        isRunning = true;
        log.info(name() + " started");
    }

    private String name() {
        return "[" + queueConfig.getQueueId() + "]";
    }

    @Override
    public QueueStats destroy() {
        isRunning = false;
        log.info("Shutting down the MBQ for " + name());
        executor.shutdownNow();
        QueueStats stats = stats();
        queue = null;
        return stats;
    }

    @Override
    public QueueStats stats() {
        log.trace("Returning stats for " + name());
        QueueStats stats = new QueueStats();
        stats.setCacheType(queueConfig.cacheType());
        stats.setCorePoolSize(executor.getCorePoolSize());
        stats.setMaxPoolSize(executor.getMaximumPoolSize());
        stats.setLargestPoolSize(executor.getLargestPoolSize());
        stats.setJobsProcessed(executor.getCompletedTaskCount());
        stats.setKeepAliveTime(executor.getKeepAliveTime(TimeUnit.SECONDS));
        stats.setRunningJobs(executor.getActiveCount());
        stats.setMode(queueConfig.mode());
        stats.setType(queueConfig.type());
        stats.setQueueId(queueConfig.getQueueId());
        stats.setShutDown(!isActive());
        stats.setShuttingDown(!isActive());
        stats.setCapacity(Integer.MAX_VALUE);
        stats.setSize(0);
        stats.setAverageProcessingTime(-1);
        stats.setThroughput(-1);
        return stats;
    }

    @Override
    public void update(QueueConfig config) {
        if (config.has("core_size")) {
            log.trace("Changing core size for " + name());
            executor.setCorePoolSize(JSONUtils.doubleToInt("core_size", config, executor.getCorePoolSize()));
        }
        if (config.has("max_size")) {
            log.trace("Changing max size for " + name());
            executor.setMaximumPoolSize(JSONUtils.doubleToInt("max_size", config, executor.getMaximumPoolSize()));
        }
        if (config.has("wait_time")) {
            log.trace("Changing wait time for " + name());
            executor.setKeepAliveTime(JSONUtils.doubleToInt("wait_time", config, (int) executor.getKeepAliveTime(SECONDS)), SECONDS);
        }
    }

    @Override
    public boolean isActive() {
        return isRunning;
    }

    @Override
    public Executor getAssociatedExecutor() {
        return executor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Future<T> enqueue(final Job<T> job) {
        return (Future<T>) executor.submit(JobContext.getContext().decorate((Job) job));
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletionService<?> enqueue(Job... jobs) {
        if (true)
            throw new UnsupportedOperationException("Not implemented yet");
        ExecutorCompletionService<Object> ecs = new ExecutorCompletionService<>(executor);
        final ExecutionContext callerContext = JobContext.getContext();
        for (Job job : jobs) {
            final Job j = job;
            ecs.submit(new AbstractJob<Object>() {
                @Override
                protected Object doCall() throws Exception {
                    try {
                        JobContext.setContext(callerContext);
                        Object result = j.call();
                        JobContext.getContext().put(j.toString(), new JobContext.JobResult(j, result));
                        return result;
                    } finally {
                        JobContext.removeContext();
                    }
                }
            });
        }
        return ecs;
    }

    public static class MBQFutureTask<V> extends FutureTask<V> {

        private final Callable<V> callable;
        private final Runnable runnable;

        public MBQFutureTask(Callable<V> callable) {
            super(callable);
            this.callable = callable;
            this.runnable = null;
        }

        public MBQFutureTask(Runnable runnable, V result) {
            super(runnable, result);
            this.callable = null;
            this.runnable = runnable;
        }

        public Callable<V> getCallable() {
            return callable;
        }

        public Runnable getRunnable() {
            return runnable;
        }
    }

    private static class MBQThreadPoolExecutor extends ThreadPoolExecutor {

        @Override
        protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
            return new MBQFutureTask<>(runnable, value);
        }

        @Override
        protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
            return new MBQFutureTask<>(callable);
        }

        public MBQThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        }
    }
}
