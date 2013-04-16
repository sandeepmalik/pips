package com.open.qbes.api.http;

import com.open.utils.Log;
import org.apache.http.client.methods.HttpPost;

import java.net.URI;

public class ManagedHttpPost extends HttpPost implements AutoCloseable {

    private static final Log log = Log.getLogger(ManagedHttpGet.class);

    public ManagedHttpPost() {
    }

    public ManagedHttpPost(URI uri) {
        super(uri);
    }

    public ManagedHttpPost(String uri) {
        super(uri);
    }

    @Override
    public void close() throws Exception {
        log.trace("Closing connection for %s", this.toString());
        releaseConnection();
    }
}
