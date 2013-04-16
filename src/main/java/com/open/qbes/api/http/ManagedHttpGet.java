package com.open.qbes.api.http;

import com.open.utils.Log;
import org.apache.http.client.methods.HttpGet;

import java.net.URI;

public class ManagedHttpGet extends HttpGet implements AutoCloseable {

    private static final Log log = Log.getLogger(ManagedHttpGet.class);

    public ManagedHttpGet() {
    }

    public ManagedHttpGet(URI uri) {
        super(uri);
    }

    public ManagedHttpGet(String uri) {
        super(uri);
    }

    @Override
    public void close() throws Exception {
        log.trace("Closing connection for %s", this.toString());
        this.releaseConnection();
    }
}
