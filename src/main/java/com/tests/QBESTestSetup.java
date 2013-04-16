package com.tests;

import com.open.qbes.QBES;
import com.open.qbes.core.ExecutionContext;
import com.open.qbes.core.JobContext;
import com.open.utils.Log;
import com.startup.QBESServer;
import com.startup.StartupOptions;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import static org.apache.log4j.Level.DEBUG;

public class QBESTestSetup {

    public static void setup() {
        JobContext.setContext(Initiator.SERVER_CONTEXT);
    }

    private static final Log log = Log.getLogger(QBESTestSetup.class);

    private static class Initiator {

        private static ExecutionContext SERVER_CONTEXT;

        static {
            try {
                QBESServer.prepare(new String[0]);

                prepare();
                QBESServer.start();
                QBES.waitForSystemStart(30);
                SERVER_CONTEXT = JobContext.getContext();
            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalStateException("Could not start server in 30 secs");
            }
            log.info("Server started. Tests begin");
        }
    }

    private static void prepare() {
        log.debug("Setting com.open.tests logger level to DEBUG");
        Log.getLogger("com.open.qbes.test").getInnerLogger().setLevel(Level.DEBUG);
        log.info("Setting the logger com.open.fixtures level to DEBUG");
        Log.getLogger("com.open.fixtures").getInnerLogger().setLevel(DEBUG);

        if ("TRUE".equalsIgnoreCase(StartupOptions.suppressServerLogsInTests)) {
            log.debug("Suppressing server logs during tests");
            Logger.getRootLogger().setLevel(Level.WARN);
        }
    }

    private static class ShutdownHandle {

        private static boolean isShutdown;

        static {
            try {
                QBESServer.stop();
            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalStateException("Could not stop server");
            }
            log.info("Server stopped successfully. Tests end");
        }
    }

    public static boolean teardown() {
        return ShutdownHandle.isShutdown;
    }
}
