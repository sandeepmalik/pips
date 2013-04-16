package com.open.utils;

import com.open.qbes.conf.QueueConfig;
import org.codehaus.jackson.map.ObjectMapper;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class JSONUtils {

    public static int doubleToInt(String key, Map<String, Object> jsonData, int defaultVal) {
        if (jsonData.containsKey(key)) {
            return new BigDecimal(jsonData.get(key).toString()).intValue();
        } else return defaultVal;
    }

    public static long doubleToLong(Object aDouble, long defaultVal) {
        if (aDouble == null)
            return defaultVal;
        return new BigDecimal(aDouble.toString()).longValue();
    }

    public static long doubleToLong(Object aDouble) {
        return new BigDecimal(aDouble.toString()).longValue();
    }

    @SuppressWarnings("unchecked")
    public static <T> int doubleToInt(String key, QueueConfig jsonData, int defaultVal) {
        return doubleToInt(key, (Map<String, Object>) jsonData, defaultVal);
    }

    public static Map<String, Object> map(Pair<String, ?>... pairs) {
        Map<String, Object> map = new HashMap<>();
        for (Pair<String, ?> pair : pairs) {
            map.put(pair.getItem1(), pair.getItem2());
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> map(Object bean) {
        if (bean instanceof Pair)
            return map(new Pair[]{(Pair) bean});
        return new ObjectMapper().convertValue(bean, Map.class);
    }


}
