package com.open.qbes.api.http;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.open.qbes.QBES;
import com.open.utils.JSONUtils;
import com.open.utils.Log;
import com.open.utils.Pair;
import com.startup.QBESServer;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.core.spi.scanning.PackageNamesScanner;
import com.sun.jersey.core.spi.scanning.Scanner;

import java.util.Arrays;

public class QBESApplicationConfig extends PackagesResourceConfig {

    private static final Log log = Log.getLogger(QBESApplicationConfig.class);

    public QBESApplicationConfig() {
        this(true);
    }

    public QBESApplicationConfig(boolean doInitDone) {
        // never mind the package names
        super("N/A");
        this.doInitDone = doInitDone;
    }

    private final boolean doInitDone;

    @Override
    public void init(Scanner scanner) {
        try {
            QBESServer.prepare(new String[0]);
            QBES.main(null);
            Iterable<String> packagesIterator = Splitter.on(",").split(System.getProperty("qbes.jobs.packages"));
            String[] packages = Iterables.toArray(packagesIterator, String.class);
            log.info("Initializing HTTP endpoints from packages %s", Arrays.asList(packages));
            super.init(new PackageNamesScanner(packages));
            setPropertiesAndFeatures(JSONUtils.map(Pair.pair(PROPERTY_RESOURCE_FILTER_FACTORIES, new QBESResourceFiltersFactory())));
            if (doInitDone) {
                QBES.initDone();
                log.info("System latch is down. QBES ready");
            }
        } catch (Throwable e) {
            throw new IllegalStateException("QBES didn't start properly", e);
        }
    }
}
