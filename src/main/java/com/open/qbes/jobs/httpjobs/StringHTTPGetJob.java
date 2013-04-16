package com.open.qbes.jobs.httpjobs;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.open.qbes.api.http.Constants;
import com.open.qbes.api.http.GlobalHTTPClient;
import com.open.qbes.api.http.ManagedHttpGet;
import com.open.qbes.core.AbstractJob;
import com.open.utils.Log;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.util.EntityUtils;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Map;

import static java.net.URLEncoder.encode;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

public class StringHTTPGetJob extends AbstractJob<String> {

    private static final Log log = Log.getLogger(StringHttpPostJob.class);

    private String getURL;

    public StringHTTPGetJob(String getURL, final Map<String, Object> queryParams) {
        if (queryParams != null) {
            getURL += ("?" + Joiner.on("&").join(Iterables.transform(queryParams.keySet(), new Function<String, Object>() {
                @Override
                public Object apply(String input) {
                    try {
                        return encode(input, "UTF-8") + "=" + encode(queryParams.get(input).toString(), "UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        throw new IllegalArgumentException(e);
                    }
                }
            })));
        }
        this.getURL = getURL;
    }

    @Override
    public String doCall() throws Exception {
        HttpClient client = GlobalHTTPClient.getInstance().get();

        try (ManagedHttpGet managedHttpGet = new ManagedHttpGet(getURL)) {
            managedHttpGet.setHeader("Accept", APPLICATION_JSON);
            managedHttpGet.setHeader("Accept-Charset", "utf-8");
            managedHttpGet.setHeader("Content-Type", Constants.APPLICATION_JSON_CHARSET_UTF_8);

            log.debug("Getting URL %s", getURL);
            HttpResponse response = client.execute(managedHttpGet);
            HttpEntity entity = response.getEntity();
            log.trace("Response received %s", getURL);
            String resp = EntityUtils.toString(entity, Charset.forName("UTF-8"));

            if (response.getStatusLine().getStatusCode() == 200) {
                return resp;
            } else
                throw new RuntimeException("Error occurred with status code " + response.getStatusLine().getStatusCode() + "\nError Response:" + resp);
        } catch (Exception e) {
            log.error("Could not GET URL for url '%s', and message: [%s]", getURL, e.getMessage());
            throw e;
        }
    }

}
