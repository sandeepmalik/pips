package com.open.qbes;

import com.open.fixtures.TestContext;
import com.open.qbes.api.http.GlobalHTTPClient;
import com.open.qbes.core.DefaultJobFactory;
import com.open.qbes.core.JobContext;
import com.open.qbes.core.QueueService;
import com.open.qbes.core.annotations.ContextConfiguration;
import com.open.qbes.persistence.DB;
import com.open.utils.Log;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import static com.open.fixtures.TestContext.getTestPackages;
import static java.lang.System.getProperty;

public class QBES {

    private final static Log log = Log.getLogger(QBES.class);

    public static JobContext INIT_CONTEXT;

    private static final CountDownLatch systemStartLatch = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {

        fixJavaLoggingIssue();

        initProperties();

        // check test context:
        if (TestContext.isRunningTests()) {
            log.info("Running QBES in test mode. Loading the fixture classes");
            String packages = System.getProperty("qbes.jobs.packages", "");
            if (packages.length() == 0)
                System.setProperty("qbes.jobs.packages", getTestPackages());
            else
                System.setProperty("qbes.jobs.packages",
                        packages + "," + getTestPackages());
        }

        initJobMappings();
        initQueues();
        initInitialContext();

        initHTTPPool();
        initPersistence();

        initStartupJobs();
    }

    public static void initStartupJobs() throws Exception {
        // init the service:
        QueueService.getInstance().runInitJobs();
    }

    public static void initHTTPPool() {
        if (getProperty("http.use-connection-pool") != null) {
            GlobalHTTPClient.init();
        }
    }

    public static void initPersistence() throws Exception {
        log.info("Initing database interface");
        DB.init();
    }

    public static void fixJavaLoggingIssue() {
        LogManager.getLogManager().reset();

        Logger globalLogger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        globalLogger.setLevel(Level.WARNING);
    }

    public static void initDone() {
        log.info("QBES startup done");
        systemStartLatch.countDown();
    }

    public static void initJobMappings() throws Exception {
        log.debug("Initing job mappings");
        QueueService.getInstance().reloadMappings();
    }

    public static void initQueues() throws Exception {
        QueueService.getInstance().init();
    }

    public static void initProperties() throws Exception {
        QueueService.getInstance().initProperties();
    }

    public static void initInitialContext() {
        log.info("Setting the initial context");
        INIT_CONTEXT = new JobContext<>(
                QueueService.getInstance().getDefaultQueueConfig(),
                new ConcurrentHashMap<String, Object>(), "[Context/0]",
                ContextConfiguration.Strategy.EXECUTE_ALL,
                new DefaultJobFactory());
        JobContext.setContext(INIT_CONTEXT);
    }

    public static boolean isStarted() {
        return systemStartLatch.getCount() == 0;
    }

    public static void waitForSystemStart(long timeout) throws InterruptedException {
        systemStartLatch.await(timeout, TimeUnit.SECONDS);
    }
}
