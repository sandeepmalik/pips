package com.open.qbes.core.cron;

import com.open.qbes.core.SystemShutdownListener;

import java.text.ParseException;
import java.util.concurrent.*;

public class CronScheduledExecutor extends ScheduledThreadPoolExecutor implements SystemShutdownListener {

    public CronScheduledExecutor(int corePoolSize) {
        super(corePoolSize);
    }

    public CronScheduledExecutor(int corePoolSize, ThreadFactory threadFactory) {
        super(corePoolSize, threadFactory);
    }

    public CronScheduledExecutor(int corePoolSize, RejectedExecutionHandler handler) {
        super(corePoolSize, handler);
    }

    public CronScheduledExecutor(int corePoolSize, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, threadFactory, handler);
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(final Runnable runnable, final RunnableScheduledFuture<V> task) {
        return new CronRunnableScheduledFuture<>(task, ((CronCommand) runnable).cronExpression);
    }

    public ScheduledFuture<?> scheduleCron(Runnable command, String cronExpression) {
        return scheduleWithFixedDelay(new CronCommand(null, command, cronExpression), 0, 1, TimeUnit.MILLISECONDS);
    }

    public ScheduledFuture<?> scheduleCron(Callable<?> command, String cronExpression) {
        return scheduleWithFixedDelay(new CronCommand(command, null, cronExpression), 0, 1, TimeUnit.MILLISECONDS);
    }

    @Override
    public void halt() {
        shutdown();
    }

    private static class CronCommand implements Runnable {

        private final Callable<?> callable;
        private final Runnable runnable;
        private final CronExpression cronExpression;

        private CronCommand(Callable<?> callable, Runnable runnable, String cronExpression) {
            this.callable = callable;
            this.runnable = runnable;
            try {
                this.cronExpression = new CronExpression(cronExpression);
            } catch (ParseException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public void run() {
            if (runnable != null) {
                runnable.run();
            } else {
                try {
                    callable.call();
                } catch (Exception ignore) {
                }
            }
        }
    }
}
