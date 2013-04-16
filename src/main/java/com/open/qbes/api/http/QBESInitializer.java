package com.open.qbes.api.http;

import com.open.qbes.QBES;
import com.open.qbes.core.QueueService;
import com.open.utils.Log;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class QBESInitializer implements ServletContextListener {

    private static final Log log = Log.getLogger(QBESInitializer.class);

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        log.info("Initializing QBES");
        try {
            QBES.main(null);
        } catch (Throwable e) {
            e.printStackTrace();
            log.fatal("Could not start QBES", e);
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        log.info("Stopping QBES");
        QueueService.getInstance().shutdown();
    }
}
