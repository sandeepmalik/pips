package com.tests.core;

import com.google.gson.reflect.TypeToken;
import com.open.qbes.api.http.Constants;
import com.open.qbes.core.annotations.ContextConfiguration;
import com.open.qbes.queues.QueueStats;
import org.testng.annotations.Test;

import java.util.Map;

import static com.tests.utils.Http.get;
import static com.tests.utils.Http.post;
import static com.open.utils.JSONUtils.map;
import static com.open.utils.Pair.pair;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@ContextConfiguration
public class QueueTest extends com.tests.QBESTest {

    private final String queue_id = "test_rl_queue";

    @Test
    public void testCreation() throws Exception {
        QueueStats stats = post(decorate(queueURL(queue_id) + "/create"),
                map(
                        pair("queue_id", queue_id),
                        pair("queue_type", "mbq"),
                        pair("cache", "memory"),
                        pair("mode", "concurrent")

                ),
                new TypeToken<QueueStats>() {
                }.getType()
        );
        assertTrue(stats.getCorePoolSize() >= Runtime.getRuntime().availableProcessors());
        assertEquals(stats.getMode(), "concurrent");
        assertEquals(stats.getType(), "mbq");
        assertEquals(stats.getCacheType(), "memory");
        assertEquals(stats.getMaxPoolSize(), Integer.MAX_VALUE);
        assertEquals(stats.isShutDown(), false);
        assertEquals(stats.isShuttingDown(), false);
        assertEquals(stats.getRunningJobs(), 0);
        assertEquals(stats.getQueueId(), queue_id);
        assertEquals(stats.getJobsProcessed(), 0);
        assertEquals(stats.getKeepAliveTime(), 60);
    }

    @Test(dependsOnMethods = {"testCreation"})
    public void testUpdate() throws Exception {
        QueueStats stats = post(decorate(queueURL(queue_id) + "/update"),
                map(
                        pair("queue_id", queue_id),
                        pair("core_size", 1),
                        pair("max_size", 10),
                        pair("wait_time", 5)

                ),
                new TypeToken<QueueStats>() {
                }.getType()
        );
        assertEquals(stats.getCorePoolSize(), 1);
        assertEquals(stats.getMode(), "concurrent");
        assertEquals(stats.getType(), "mbq");
        assertEquals(stats.getCacheType(), "memory");
        assertEquals(stats.getMaxPoolSize(), 10);
        assertEquals(stats.isShutDown(), false);
        assertEquals(stats.isShuttingDown(), false);
        assertEquals(stats.getRunningJobs(), 0);
        assertEquals(stats.getQueueId(), queue_id);
        assertEquals(stats.getJobsProcessed(), 0);
        assertEquals(stats.getKeepAliveTime(), 5);
    }

    @Test(dependsOnMethods = {"testCreation"})
    public void testStatus() throws Exception {
        QueueStats stats = get(decorate(queueURL(queue_id)), null,
                new TypeToken<QueueStats>() {
                }.getType()
        );
        assertEquals(stats.getMode(), "concurrent");
        assertEquals(stats.getType(), "mbq");
        assertEquals(stats.getCacheType(), "memory");
        assertEquals(stats.isShutDown(), false);
        assertEquals(stats.isShuttingDown(), false);
        assertEquals(stats.getQueueId(), queue_id);
    }

    @Test(dependsOnMethods = "testUpdate")
    public void testEnqueue() throws Exception {
        Map<String, Object> resp = post(decorate(queueURL(queue_id) + "/enqueue"),
                map(
                        pair("queue_id", queue_id),
                        pair("job_type", "echo")

                )
        );
        assertEquals(resp.get(Constants.STATUS_KEY), "ENQUEUED");
        assertTrue(resp.containsKey("enqueued_at"));
    }
}
