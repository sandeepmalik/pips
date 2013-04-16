package com.open.utils;

import static java.lang.Integer.toHexString;

public class StringUtils {
    public static String asString(Object o) {
        String toString = o.toString();
        String hex = toHexString(o.hashCode());
        if (toString.contains("@" + hex)) {
            String[] parts = toString.split("\\.");
            if (parts.length == 0)
                return o.toString();
            else {
                String part = parts[parts.length - 1];
                if (part.contains("$"))
                    return part.replace("@", "[").concat("]");
                else return part.replace("@" + hex, "");
            }
        } else {
            return o.toString();
        }
    }
}
