package com.open.utils;

import com.open.qbes.persistence.DB;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static java.sql.Types.*;

public class SyncUtils {

    private static Log log = Log.getLogger(SyncUtils.class);

    public static int quietParseInt(String s) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException ignore) {
            return 0;
        }
    }

    public static long quietParseLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ignore) {
            return 0;
        }
    }

    public static <V> Object valueOf(Map<String, V> data, String... keys) {
        Map<String, V> currentNode = data;
        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            if (currentNode == null || !currentNode.containsKey(key))
                return null;
            if (i != keys.length - 1)
                currentNode = (Map<String, V>) data.get(key);
        }
        return currentNode.get(keys[keys.length - 1]);
    }

    public static java.util.Date parseDate(SimpleDateFormat formatter, Object value) throws ParseException {
        if (value == null)
            return null;
        else return formatter.parse(value.toString());
    }

    public static long[] reserveIds(String sequence, int howMany) {
        log.debug("Reserving %s ids from %s", howMany, sequence);
        List<Map<String, Object>> data = DB.fetch("SELECT nextval('" + sequence + "') FROM generate_series(1," + howMany + ");");
        long[] ids = new long[data.size()];
        for (int i = 0; i < data.size(); i++) {
            Map<String, Object> stringObjectMap = data.get(i);
            ids[i] = (Long) stringObjectMap.get("nextval");
        }
        return ids;
    }

    public static java.sql.Timestamp getCurrentJavaSqlTimestamp() {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        return new java.sql.Timestamp(calendar.getTimeInMillis());
    }

    public static void handleNullAndSet(List<Object> data, Object value, int type, Object defaultValue) throws SQLException, ParseException {
        if (value != null) {
            switch (type) {
                case VARCHAR:
                    data.add(value.toString());
                    break;
                case BIGINT:
                    data.add(Long.parseLong(value.toString()));
                    break;
                case INTEGER:
                    data.add(Integer.parseInt(value.toString()));
                    break;
                case DATE:
                    if (value instanceof java.util.Date) {
                        data.add(new Date(((java.util.Date) value).getTime()));
                    } else data.add(value);
                    break;
                case BOOLEAN:
                    data.add(value == Boolean.TRUE);
                    break;
                case FLOAT:
                    data.add(value);
                    break;
                case DOUBLE:
                    data.add(value);
                    break;
                default:
                    log.error("Unhandled type " + type);
                    throw new UnsupportedOperationException();
            }
        } else if (defaultValue != null) {
            data.add(defaultValue);
        } else data.add(null);
    }

    public static void safeSetItem(PreparedStatement p, Object value, int type, Object defaultValue, int index) throws SQLException, ParseException {
        if (value == null) {
            if (defaultValue != null)
                value = defaultValue;
            else {
                p.setObject(index, null);
                return;
            }
        }
        switch (type) {
            case VARCHAR:
                p.setString(index, value.toString());
                break;
            case BIGINT:
                p.setLong(index, ((Number) value).longValue());
                break;
            case INTEGER:
                p.setInt(index, ((Number) value).intValue());
                break;
            case DATE:
                p.setDate(index, new Date(((java.util.Date) value).getTime()));
                break;
            case BOOLEAN:
                p.setBoolean(index, Boolean.valueOf(value.toString()));
                break;
            case TIMESTAMP:
                p.setTimestamp(index, (Timestamp) value);
                break;
            case FLOAT:
                p.setFloat(index, ((Number) value).floatValue());
                break;
            case DOUBLE:
                p.setDouble(index, ((Number) value).doubleValue());
                break;
            default:
                log.error("Unhandled type " + type);
                throw new UnsupportedOperationException();
        }
    }

    public static void safeSetItem(PreparedStatement p, Object value, int type, int index) throws SQLException, ParseException {
        safeSetItem(p, value, type, null, index);
    }

    public static void handleNullAndSet(List<Object> data, Object value, int type) throws SQLException, ParseException {
        handleNullAndSet(data, value, type, null);
    }

    public static void handleNullAndSet(List<Object> row, Map<String, Object> data, String column, int type) throws SQLException, ParseException {
        handleNullAndSet(row, data.get(column), type);
    }
}
