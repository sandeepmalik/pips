package com.open.utils;

import com.open.qbes.core.UncheckedSQLException;

import java.sql.SQLException;

import static com.google.common.base.Throwables.getStackTraceAsString;

public class ExceptionUtils {

    public static String getExceptionString(Throwable t) {
        if (t instanceof UncheckedSQLException && t.getCause() instanceof SQLException)
            return getExceptionString(t.getCause());
        StringBuilder stringBuilder = new StringBuilder();
        if (t instanceof SQLException) {
            while (t != null) {
                stringBuilder.append(getStackTraceAsString(t));
                t = SQLException.class.cast(t).getNextException();
                if (t != null)
                    stringBuilder.append(" Next => ");
            }
            return stringBuilder.toString();
        } else return getStackTraceAsString(t);
    }
}
