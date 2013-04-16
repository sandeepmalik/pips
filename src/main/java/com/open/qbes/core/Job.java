package com.open.qbes.core;

import java.util.Map;
import java.util.concurrent.Callable;

public interface Job<T> extends Callable<T>, Comparable<Job<T>> {

    String getId();

    int priority();

    void validate(Map<String, Object> jobData);

    void onComplete(JobCallback callbackJob);
}
