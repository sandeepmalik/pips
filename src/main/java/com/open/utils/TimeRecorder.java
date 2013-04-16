package com.open.utils;

import java.util.Date;

public class TimeRecorder {

    private static final Log log = Log.getLogger(TimeRecorder.class);

    private static final ThreadLocal<Long> TIME_RECORDER = new ThreadLocal<Long>() {

        private Long value;

        @Override
        public Long get() {
            return value;
        }

        @Override
        public void set(Long value) {
            this.value = value;
        }
    };

    public static void start() {
        TIME_RECORDER.set(new Date().getTime());
    }

    public static long diff() {
        return new Date().getTime() - TIME_RECORDER.get();
    }

    public static void report() {
        log.debug("Time taken " + diff() + " ms");
    }

}
