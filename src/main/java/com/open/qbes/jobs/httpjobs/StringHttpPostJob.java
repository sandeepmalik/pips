package com.open.qbes.jobs.httpjobs;

import com.open.qbes.api.http.Constants;
import com.open.qbes.api.http.StatusCode;
import com.open.qbes.api.http.GlobalHTTPClient;
import com.open.qbes.api.http.ManagedHttpPost;
import com.open.qbes.core.AbstractJob;
import com.open.qbes.core.exceptions.APIException;
import com.open.utils.Log;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import java.nio.charset.Charset;

public class StringHttpPostJob extends AbstractJob<String> {

    private static final Log log = Log.getLogger(StringHttpPostJob.class);

    private final String postData;
    private final String returnURL;
    private final boolean discardResponse;

    public StringHttpPostJob(String postData, String returnURL, boolean discardResponse) {
        this.postData = postData;
        this.returnURL = returnURL;
        this.discardResponse = discardResponse;
    }

    @Override
    public String doCall() throws Exception {
        log.trace("Got a post url job");
        HttpClient client = GlobalHTTPClient.getInstance().get();
        log.trace("post url ready %s", returnURL);

        try (ManagedHttpPost managedHttpPost = new ManagedHttpPost(returnURL)) {
            managedHttpPost.setEntity(new StringEntity(postData, Charset.forName("UTF-8")));
            managedHttpPost.setHeader("Accept", "application/json");
            managedHttpPost.setHeader("Accept-Charset", "utf-8");
            managedHttpPost.setHeader("Content-Type", Constants.APPLICATION_JSON_CHARSET_UTF_8);

            log.debug("Posting URL %s", returnURL);
            HttpResponse response = client.execute(managedHttpPost);
            HttpEntity entity = response.getEntity();
            log.trace("URL posted %s", returnURL);
            if (discardResponse) {
                EntityUtils.consumeQuietly(entity);
                return null;
            } else {
                String resp = EntityUtils.toString(entity, Charset.forName("UTF-8"));
                if (response.getStatusLine().getStatusCode() == 200) {
                    return resp;
                } else {
                    throw new APIException(StatusCode.fromCode(response.getStatusLine().getStatusCode()), resp);
                }
            }
        } catch (Exception e) {
            log.error("Could not POST URL for url '%s', and message: [%s], and data %s", e, returnURL, e.getMessage(), postData);
            throw e;
        }
    }
}
