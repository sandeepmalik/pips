package com.tests.utils;

import com.google.gson.Gson;
import com.open.qbes.jobs.httpjobs.StringHTTPGetJob;
import com.open.qbes.jobs.httpjobs.StringHttpPostJob;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public final class Http {

    @SuppressWarnings("unchecked")
    public static <T> T get(String url, Map<String, Object> queryParams, Type returnedType) throws Exception {
        return new Gson().fromJson(new StringHTTPGetJob(url, queryParams).call(), returnedType);
    }

    public static Map<String, Object> get(String url) throws Exception {
        return get(url, null, HashMap.class);
    }

    public static Map<String, Object> get(String url, Map<String, Object> queryParams) throws Exception {
        return get(url, queryParams, HashMap.class);
    }

    @SuppressWarnings("unchecked")
    public static <R, T> T post(String url, R data, Type returnType) throws Exception {
        Gson gson = new Gson();
        String returnedJSON = new StringHttpPostJob(gson.toJson(data), url, false).call();
        return gson.fromJson(returnedJSON, returnType);
    }

    public static <R, T> T post(String url, R data) throws Exception {
        return post(url, data, HashMap.class);
    }
}
