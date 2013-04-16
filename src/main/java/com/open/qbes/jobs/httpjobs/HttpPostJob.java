package com.open.qbes.jobs.httpjobs;

import com.google.gson.Gson;
import com.open.qbes.api.http.GlobalHTTPClient;
import com.open.qbes.api.http.ManagedHttpPost;
import com.open.qbes.core.AbstractJob;
import com.open.qbes.core.annotations.SkipCreationCheck;
import com.open.utils.Log;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SkipCreationCheck
public class HttpPostJob extends AbstractJob<String> {

    private static final Log log = Log.getLogger(HttpPostJob.class);

    private final Map<String, Object> returnData;
    private final String returnURL;
    private final boolean discardContents;

    public HttpPostJob(Map<String, Object> returnData, String returnURL) {
        this(returnData, returnURL, false);
    }

    public HttpPostJob(Map<String, Object> returnData, String returnURL, boolean discardContents) {
        this.returnData = returnData;
        this.returnURL = returnURL;
        this.discardContents = discardContents;
    }

    @Override
    public String doCall() throws Exception {
        Gson gson = new Gson();
        log.trace("Got a post url job");
        HttpClient client = GlobalHTTPClient.getInstance().get();
        log.trace("post url ready %s", returnURL);

        List<NameValuePair> params = new ArrayList<>();
        if (returnData != null) {
            for (String key : returnData.keySet()) {
                params.add(new BasicNameValuePair(key, gson.toJson(returnData.get(key))));
            }
        }
        try (ManagedHttpPost managedHttpPost = new ManagedHttpPost(returnURL)) {

            if (params.size() > 0) {
                managedHttpPost.setEntity(new UrlEncodedFormEntity(params, HTTP.UTF_8));
            }

            log.debug("Posting URL %s", returnURL);
            HttpResponse response = client.execute(managedHttpPost);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("Error occurred with status code " + response.getStatusLine().getStatusCode() + "\nError Response:" + response);
            }

            HttpEntity entity = response.getEntity();
            log.trace("URL posted %s", returnURL);
            if (discardContents) {
                EntityUtils.consumeQuietly(entity);
                return null;
            } else return EntityUtils.toString(entity, Charset.forName("UTF-8"));
        } catch (Exception e) {
            log.error("Could not POST URL for url '%s', and message: [%s], and data %s", e, returnURL, e.getMessage(), returnData);
        }
        return null;
    }

}
