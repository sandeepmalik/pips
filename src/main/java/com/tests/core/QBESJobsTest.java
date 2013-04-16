package com.tests.core;

import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.open.qbes.core.QueueService.*;
import static com.open.qbes.jobs.ReturnURLJob.RETURN_URL_KEY;
import static com.tests.utils.Http.post;
import static com.open.utils.JSONUtils.map;
import static com.open.utils.Pair.pair;
import static org.testng.Assert.assertEquals;

public class QBESJobsTest extends com.tests.QBESTest {

    @Test
    public void testReturnURLJob() throws Exception {
        checkReturnedData("testReturnURLJob", String.valueOf(System.currentTimeMillis()));
    }

    private void checkReturnedData(String id, String someData) throws Exception {
        String returnURL = context() + "/tests/" + id;

        Future<Map<String, Object>> future = register(id);


        post(decorate(queueURL(DEFAULT_QUEUE) + "/enqueue"),
                map(
                        pair(QUEUE_ID, DEFAULT_QUEUE),
                        pair(JOB_TYPE, RETURN_URL_KEY),
                        pair(RETURN_URL_KEY, returnURL),
                        pair("some_data", someData)

                )
        );

        Map<String, Object> resp = future.get(5, TimeUnit.SECONDS);
        assertEquals(resp.get("api_key"), System.getProperty("api_key"));
        assertEquals(resp.get("some_data"), someData);
        System.out.println(resp);
    }

    @Test
    public void testStringHTTPPostUsesUTF8() throws Exception {
        checkReturnedData("testStringHTTPPostUsesUTF8", "ვეპხის ტყაოსანი შოთა რუსთაველი");
    }
}
