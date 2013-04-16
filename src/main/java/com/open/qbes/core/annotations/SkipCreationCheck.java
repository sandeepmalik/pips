package com.open.qbes.core.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 11/19/12
 * Time: 3:24 PM
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SkipCreationCheck {
    boolean value() default true;
}

