package com.open.qbes.api.http;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.open.utils.Pair;

import javax.ws.rs.core.Response;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;
import static com.open.utils.JSONUtils.map;
import static javax.ws.rs.core.Response.ok;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 11/13/12
 * Time: 1:33 PM
 */
public interface Constants {
    public static final String API_VERSION = "api/v1";

    public static final String STATUS_KEY = "status";

    Gson LOWER_CASE_JSON = new GsonBuilder().setFieldNamingPolicy(LOWER_CASE_WITH_UNDERSCORES).create();

    public static final String APPLICATION_JSON_CHARSET_UTF_8 = "application/json; charset=UTF-8";

    public static class Utils {
        public static Response response(Pair<String, ?>... pairs) {
            return ok(map(pairs)).build();
        }
    }
}
