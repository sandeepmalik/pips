package com.open.qbes.core;

public interface RequiresContextualLogInfo<T> {

    public T getInfo();

    <T> T get(String key, Class<T> valueType);

    void setContextInfo(T contextInfo);
}
