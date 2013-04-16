package com.tests.rl;

import com.open.utils.Log;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.tests.TestSetup.setup;

@Test
public class RateLimitTest {

    private static Log log = Log.getLogger(RateLimitTest.class);

    private final String queue_id = "test_rl_queue";
    private final String queue_url = "/queue/" + queue_id;

    @BeforeClass
    public void setUp() {
        setup();
    }

    /*@Test
    public void testQueueCreation() throws Exception {
        Map<String, Object> resp = post(forURL(queue_url + "/create"),
                map(
                        pair("queue_id", queue_id),
                        pair("queue_type", "rlq"),
                        pair("cache", "db"),
                        pair("mode", "single"),
                        pair("throughput_per_day", "1")

                )
        );
        assertNotNull(resp);
        assertEquals(resp.get("status"), "CREATED");
    }*/

    /*@Test(dependsOnMethods = {"testQueueCreation"}, alwaysRun = true)
    public void testEnqueue() throws Exception {
        Map<String, Object> resp = post(forURL(queue_url + "/enqueue"),
                map(
                        pair("queue_id", queue_id),
                        pair("job_type", "ReturnURLJob"),
                        pair("return_url", "null")

                )
        );
        assertNotNull(resp);
        assertEquals(resp.get("status"), "Enqueued");
    }*/

    /*@Test(dependsOnMethods = {"testQueueCreation"})
    public void testManyEnqueue() throws Exception {

        TimeRecorder.start();
        for (int i = 0; i < 1; i++) {
            post(forURL(queue_url + "/enqueue"),
                    map(
                            pair("queue_id", queue_id),
                            pair("job_type", "ReturnURLJob"),
                            pair("return_url", "null")

                    )
            );
        }
        log.info("Time taken %d ms", TimeRecorder.diff());
    }*/
}
