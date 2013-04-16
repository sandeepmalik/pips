package com.open.qbes.core.annotations;

import com.open.qbes.core.QueueService;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 10/29/12
 * Time: 3:31 PM
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface RunsOnInit {
    String queue() default QueueService.DEFAULT_QUEUE;

    boolean blocks() default false;

    String[] modules() default {};

    boolean runsInTestModeOnly() default false;
}
