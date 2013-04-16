package com.tests;

import com.open.fixtures.TestContext;
import com.tests.api.QBESClientProxyAPI;
import com.open.utils.Log;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.util.concurrent.Future;

import static com.tests.QBESTestSetup.setup;
import static com.tests.QBESTestSetup.teardown;

public abstract class QBESTest {

    protected final Log log = Log.getLogger(this.getClass());

    @BeforeSuite
    protected void setUp() throws Exception {
        setup();
    }

    public String context() {
        return TestContext.context();
    }

    public String decorate(String url) {
        return context() + url;
    }

    public String queueURL(String queueId) {
        return "/queue/" + queueId;
    }

    @AfterSuite
    protected void tearDown() {
        teardown();
    }

    protected <T> Future<T> register(String id) {
        return QBESClientProxyAPI.register(id);
    }
}
