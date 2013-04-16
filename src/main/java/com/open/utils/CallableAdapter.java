package com.open.utils;

import java.util.concurrent.Callable;

public final class CallableAdapter {

    public static Runnable asRunnable(final Callable callable) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    callable.call();
                } catch (Exception ignore) {
                }
            }
        };
    }

}
