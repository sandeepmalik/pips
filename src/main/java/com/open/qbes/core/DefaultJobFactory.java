package com.open.qbes.core;

public class DefaultJobFactory implements JobFactory {
    @Override
    public <T, J extends Job<T>> J create(Class<J> jobClass) throws Exception {
        return jobClass.newInstance();
    }
}
