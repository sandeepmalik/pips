package com.open.qbes.core;

public class UncheckedSQLException extends RuntimeException {

    public UncheckedSQLException() {
    }

    public UncheckedSQLException(String message) {
        super(message);
    }

    public UncheckedSQLException(String message, Throwable cause) {
        super(message, cause);
    }

    public UncheckedSQLException(Throwable cause) {
        super(cause);
    }

    public UncheckedSQLException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
