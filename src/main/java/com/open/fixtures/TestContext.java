package com.open.fixtures;

import com.open.qbes.api.http.Constants;
import com.startup.StartupOptions;

import static com.startup.StartupOptions.loadFixtures;
import static com.startup.StartupOptions.testMode;

public final class TestContext {

    public static final boolean isRunningTests() {
        return testMode;
    }

    public static String context() {
        return "http://localhost:" + StartupOptions.port + StartupOptions.context + "/" + Constants.API_VERSION;
    }

    public static String baseContext() {
        return "http://localhost:" + StartupOptions.port + StartupOptions.context;
    }

    public static String getTestPackages() {
        return loadFixtures ?
                "com.open.fixtures,com.tests.api" : "com.tests.api";
    }
}
