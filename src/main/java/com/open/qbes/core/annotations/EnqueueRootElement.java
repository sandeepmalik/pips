package com.open.qbes.core.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 11/20/12
 * Time: 9:34 AM
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface EnqueueRootElement {

    public static final String USE_UNQUALIFIED_CLASS_NAME = "USE_UNQUALIFIED_CLASS_NAME";

    String value() default USE_UNQUALIFIED_CLASS_NAME;
}
