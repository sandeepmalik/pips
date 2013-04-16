package com.open.utils;

import org.apache.log4j.Logger;

import java.util.Formatter;

public final class Log {

    private final Logger log;

    public Log(Logger log) {
        this.log = log;
    }

    public static Log getLogger(String name) {
        return new Log(Logger.getLogger(name));
    }

    public static Log getLogger(Class clazz) {
        return new Log(Logger.getLogger(clazz));
    }

    public void trace(String format, Object... args) {
        if (log.isTraceEnabled()) {
            log.trace(ContextualInfo.format() + new Formatter().format(format, args).toString());
        }
    }

    public void trace(String format, Throwable t, Object... args) {
        if (log.isTraceEnabled()) {
            log.trace(ContextualInfo.format() + new Formatter().format(format, args).toString(), t);
        }
    }

    public void debug(String format, Object... args) {
        if (log.isDebugEnabled()) {
            log.debug(ContextualInfo.format() + new Formatter().format(format, args).toString());
        }
    }

    public void debug(String format, Throwable t, Object... args) {
        if (log.isDebugEnabled()) {
            log.debug(ContextualInfo.format() + new Formatter().format(format, args).toString(), t);
        }
    }

    public void error(String format, Object... args) {
        log.error(ContextualInfo.format() + new Formatter().format(format, args).toString());
    }

    public void error(String format, Throwable t, Object... args) {
        log.error(ContextualInfo.format() + new Formatter().format(format, args).toString(), t);
    }

    public void fatal(String format, Object... args) {
        log.fatal(ContextualInfo.format() + new Formatter().format(format, args).toString());
    }

    public void fatal(String format, Throwable t, Object... args) {
        log.fatal(ContextualInfo.format() + new Formatter().format(format, args).toString(), t);
    }

    public void info(String format, Object... args) {
        if (log.isInfoEnabled()) {
            log.info(ContextualInfo.format() + new Formatter().format(format, args).toString());
        }
    }

    public void info(String format, Throwable t, Object... args) {
        if (log.isInfoEnabled()) {
            log.info(ContextualInfo.format() + new Formatter().format(format, args).toString(), t);
        }
    }

    public void warn(String format, Object... args) {
        log.warn(ContextualInfo.format() + new Formatter().format(format, args).toString());
    }

    public void warn(String format, Throwable t, Object... args) {
        log.warn(ContextualInfo.format() + new Formatter().format(format, args).toString(), t);
    }

    public Logger getInnerLogger() {
        return log;
    }
}
