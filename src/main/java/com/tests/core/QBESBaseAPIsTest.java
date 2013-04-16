package com.tests.core;

import com.tests.QBESTest;
import com.startup.StartupOptions;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.open.fixtures.TestContext.baseContext;
import static com.tests.utils.Http.get;
import static com.open.utils.JSONUtils.map;
import static com.open.utils.Pair.pair;
import static org.testng.Assert.*;

public class QBESBaseAPIsTest extends QBESTest {

    @Test
    public void testPing() throws Exception {
        Map resp = get(baseContext() + "/ping");
        assertTrue(resp.containsKey("status"));
        assertTrue("ALIVE".equals(resp.get("status")));
    }

    @Test(dependsOnMethods = {"testPing"})
    public void testStatus() throws Exception {
        Map resp = get(baseContext() + "/status");
        assertTrue(resp.containsKey("code"));
        assertTrue(resp.containsKey("checked_at"));
        assertTrue(resp.containsKey("name"));
        assertTrue(resp.containsKey("status"));

        assertEquals(resp.get("status"), "ok");
        assertEquals(resp.get("code"), "200");
        assertEquals(resp.get("name"), StartupOptions.context);

        String dependenciesKey = "dependencies";

        if (resp.containsKey(dependenciesKey)) {
            assertTrue(resp.get(dependenciesKey) instanceof List);
            List<Map<String, Object>> dependencies = (List<Map<String, Object>>) resp.get(dependenciesKey);
            for (Map<String, Object> dependency : dependencies) {
                assertEquals(dependency.size(), 1);

                String dependencyKey = dependency.keySet().iterator().next();
                Map<String, Object> dependencyValues = (Map<String, Object>) dependency.get(dependencyKey);
                assertTrue(dependencyValues.containsKey("code"));
                assertTrue(dependencyValues.containsKey("checked_at"));
                assertTrue(dependencyValues.containsKey("name"));
                assertTrue(dependencyValues.containsKey("status"));

                assertEquals(dependencyValues.get("status"), "ok");
                assertEquals(dependencyValues.get("code"), "200");
                assertEquals(dependencyKey, dependencyValues.get("name"));
            }
        }
    }

    @Test(dependsOnMethods = {"testStatus"})
    public void testSystemStarted() throws Exception {
        Map resp = get(baseContext() + "/is_started");
        assertTrue(resp.containsKey("started"));
        assertTrue("TRUE".equalsIgnoreCase(resp.get("started").toString()));
    }

    @Test(dependsOnMethods = {"testSystemStarted"})
    public void testTimedSystemStarted() throws Exception {
        Map resp = get(baseContext() + "/is_started", map(pair("wait_for_start", true), pair("timeout", 1)));
        assertTrue(resp.containsKey("started"));
        assertNull(resp.get("timed_out"));
    }
}
