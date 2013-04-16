package com.startup;

import com.google.common.base.Joiner;
import com.open.qbes.QBES;
import com.open.qbes.api.http.QBESApplicationConfig;
import com.open.qbes.core.QueueService;
import com.open.utils.Log;
import com.sun.jersey.api.container.ContainerFactory;
import com.sun.jersey.server.impl.container.httpserver.HttpHandlerContainerProvider;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.log4j.PropertyConfigurator;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static com.open.utils.JSONUtils.map;
import static com.open.utils.Pair.pair;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class QBESServer {

    private static final Log log = Log.getLogger(QBESServer.class);

    private static AtomicInteger serial = new AtomicInteger();

    private static ExecutorService httpWorkers;
    private static HttpServer server;

    public static void main(String[] args) throws Exception {
        System.setProperty("args", Joiner.on(" ").join(args));
        start();
    }

    public static void prepare(String[] args) {
        StartupOptions startupOptions = new StartupOptions();

        CmdLineParser parser = new CmdLineParser(startupOptions);

        args = args.length > 0 ? args : System.getProperty("args", "").split(" ");

        if (args.length > 0 && "--help".equals(args[0]) || "-h".equals(args[0])) {
            printUsageLine(parser);
            parser.printUsage(System.out);
            return;
        }

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            printUsageLine(parser);
            throw new IllegalArgumentException("Insufficient or no arguments were provided. See QBES Usage instructions.");
        }

        PropertyConfigurator.configure(StartupOptions.configs + "/log4j.properties");
        System.setProperty("resources", StartupOptions.resources);
        System.setProperty("shared.loader", StartupOptions.configs);
    }

    private static void printUsageLine(CmdLineParser parser) {
        System.err.println("QBES Usage:");
        System.err.println("-----------");
        parser.printUsage(System.err);
    }

    public static void start() throws Exception {
        InetSocketAddress isa = new InetSocketAddress(StartupOptions.port);
        server = HttpServer.create(isa, 0);

        QBESApplicationConfig restApp = new QBESApplicationConfig(false);
        restApp.setPropertiesAndFeatures(
                map(
                        pair("com.sun.jersey.spi.container.ContainerProvider", HttpHandlerContainerProvider.class)
                )
        );

        server.createContext(StartupOptions.context, ContainerFactory.createContainer(HttpHandler.class, restApp));

        httpWorkers = newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "[HTTP Worker] - " + serial.incrementAndGet());
            }
        });

        server.setExecutor(httpWorkers);
        server.start();
        QBES.initDone();
        log.info("QBES started %s", server.getAddress());
    }

    public static void stop() {
        log.info("Stopping QBES");
        QueueService.getInstance().shutdown();
        httpWorkers.shutdownNow();
        server.stop(0);
        log.info("QBES stopped");
    }
}
