package com.open.qbes.core;

public interface CallbackCompletionService<T> {

    public void onCallbackComplete(JobCallback<T> jobCallback);

}
