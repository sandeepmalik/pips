package com.open.qbes.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface BlockingJob {

    public static final String AWAIT = "await";
    public static final String UNITS = "units";

    public static final String SYNC_LATCH = "SYNC_LATCH";

}
