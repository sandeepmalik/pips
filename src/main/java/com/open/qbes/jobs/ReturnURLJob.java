package com.open.qbes.jobs;

import com.google.gson.Gson;
import com.open.qbes.core.AbstractJob;
import com.open.qbes.core.JobContext;
import com.open.qbes.core.annotations.EnqueueRootElement;
import com.open.qbes.jobs.httpjobs.StringHttpPostJob;

import java.util.HashMap;
import java.util.Map;

import static java.lang.System.getProperty;

@EnqueueRootElement("EMAIL,FB_REQUEST,return_url")
public class ReturnURLJob extends AbstractJob<Object> {

    public static final String RETURN_URL_KEY = "return_url";

    @Override
    public void validate(Map<String, Object> jobData) {
        super.validate(jobData);
        if (!jobData.containsKey(RETURN_URL_KEY))
            throw new IllegalArgumentException("no return URL specified");
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object doCall() throws Exception {
        return returnURL(JobContext.getContext().get(RETURN_URL_KEY).toString(), new HashMap(JobContext.getContext().getInitialJobData()));
    }

    @SuppressWarnings("unchecked")
    public static Object returnURL(String returnURL, Map map) throws Exception {
        map.put("api_key", getProperty("api_key"));
        String postData = new Gson().toJson(map);
        return new StringHttpPostJob(postData, returnURL, true).call();
    }
}
