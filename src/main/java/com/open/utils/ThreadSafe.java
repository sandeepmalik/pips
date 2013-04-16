package com.open.utils;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 7/19/12
 * Time: 2:53 PM
 */
public @interface ThreadSafe {

    boolean value() default true;

}
