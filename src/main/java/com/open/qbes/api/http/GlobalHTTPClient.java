package com.open.qbes.api.http;

import com.open.utils.Log;
import com.open.utils.ThreadSafe;
import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;

import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static org.apache.http.conn.scheme.PlainSocketFactory.getSocketFactory;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 8/19/12
 * Time: 8:39 PM
 */
@ThreadSafe(true)
public class GlobalHTTPClient {

    private static GlobalHTTPClient ourInstance = new GlobalHTTPClient();

    private static final Log log = Log.getLogger(GlobalHTTPClient.class);

    private static volatile boolean poolConfigured = true;

    public static GlobalHTTPClient getInstance() {
        return ourInstance;
    }

    private GlobalHTTPClient() {
    }

    private HttpClient client;
    private PoolingClientConnectionManager poolingClientConnectionManager;

    public HttpClient get() {
        if (poolConfigured)
            return client;
        else return new DefaultHttpClient();
    }

    public void shutdown() {
        poolingClientConnectionManager.shutdown();
    }

    public static void init() {
        if (getProperty("http.pool.schemes.http") == null) {
            log.info("No http connection pool configuration found");
            poolConfigured = false;
            return;
        }

        log.info("Configuring schemes");

        SchemeRegistry schemeRegistry = new SchemeRegistry();
        String[] httpSchemes = getProperty("http.pool.schemes.http").split(",");
        String[] httpsSchemes = getProperty("http.pool.schemes.https").split(",");

        for (String httpScheme : httpSchemes) {
            log.info("Adding new HTTP scheme for port %s", httpScheme);
            schemeRegistry.register(new Scheme("http", parseInt(httpScheme), getSocketFactory()));
        }

        for (String httpsScheme : httpsSchemes) {
            log.info("Adding new HTTPS scheme for port %s", httpsScheme);
            schemeRegistry.register(new Scheme("https", parseInt(httpsScheme), SSLSocketFactory.getSocketFactory()));
        }
        getInstance().poolingClientConnectionManager = new PoolingClientConnectionManager(schemeRegistry);

        int defaultMaxPerRoute = parseInt(getProperty("http.pool.default.max.per.route"));

        getInstance().poolingClientConnectionManager.setMaxTotal(parseInt(getProperty("http.pool.max.total")));
        getInstance().poolingClientConnectionManager.setDefaultMaxPerRoute(defaultMaxPerRoute);

        String[] hosts = getProperty("http.pool.hosts").split(",");
        for (String host : hosts) {
            String[] parts = host.split("::");
            if (parts.length == 1) {
                log.info("Configuring host %s with a pool of %d connections", parts[0], defaultMaxPerRoute);
                getInstance().poolingClientConnectionManager.setMaxPerRoute(
                        new HttpRoute(new HttpHost(parts[0])), defaultMaxPerRoute);
            } else {
                int maxConnections = parseInt(parts[1]);
                log.info("Configuring host %s with a pool of %d connections", parts[0], maxConnections);
                getInstance().poolingClientConnectionManager.setMaxPerRoute(
                        new HttpRoute(new HttpHost(parts[0])), maxConnections);
            }
        }
        getInstance().client = new DefaultHttpClient(getInstance().poolingClientConnectionManager);
        log.info("HTTP Connection pool(s) ready");
    }
}
