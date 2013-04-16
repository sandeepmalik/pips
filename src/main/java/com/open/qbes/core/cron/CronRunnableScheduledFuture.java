package com.open.qbes.core.cron;

import java.util.Date;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 2/13/13
 * Time: 4:28 PM
 */
public class CronRunnableScheduledFuture<V> implements RunnableScheduledFuture<V> {

    private final RunnableScheduledFuture<V> innerRunnableScheduledFuture;
    private final CronExpression cronExpression;
    private volatile long nextRunTime;

    public CronRunnableScheduledFuture(RunnableScheduledFuture<V> innerRunnableScheduledFuture, CronExpression cronExpression) {
        this.innerRunnableScheduledFuture = innerRunnableScheduledFuture;
        this.cronExpression = cronExpression;
        nextRunTime = cronExpression.getNextValidTimeAfter(new Date()).getTime();
    }

    @Override
    public boolean isPeriodic() {
        return innerRunnableScheduledFuture.isPeriodic();
    }

    @Override
    public void run() {
        nextRunTime = cronExpression.getNextValidTimeAfter(new Date()).getTime();
        innerRunnableScheduledFuture.run();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return innerRunnableScheduledFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return innerRunnableScheduledFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
        return innerRunnableScheduledFuture.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        return innerRunnableScheduledFuture.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return innerRunnableScheduledFuture.get(timeout, unit);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(nextRunTime - new Date().getTime(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return innerRunnableScheduledFuture.compareTo(o);
    }
}
