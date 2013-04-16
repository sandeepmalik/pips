package com.open.qbes.core.annotations;

import com.open.qbes.core.DefaultJobFactory;
import com.open.qbes.core.JobFactory;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.open.qbes.core.annotations.ContextConfiguration.Strategy.FAIL_FAST;
import static com.open.qbes.core.QueueService.DEFAULT_QUEUE;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 1/9/13
 * Time: 9:37 PM
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ContextConfiguration {

    public enum Strategy {
        EXECUTE_ALL, FAIL_FAST
    }

    public enum ExecutionState {
        NOT_STARTED, RUNNING, FINISHED_NORMALLY, FINISHED_WITH_ERRORS
    }

    String queueId() default DEFAULT_QUEUE;

    Strategy strategy() default FAIL_FAST;

    Class<? extends JobFactory> jobFactory() default DefaultJobFactory.class;

    Class<? extends Map> cacheType() default ConcurrentHashMap.class;

    boolean setGetQueryParamsInCache() default true;

    String contextInfoParam() default "";

    String name() default "";
}
