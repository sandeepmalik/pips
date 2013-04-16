package com.tests.api;


import com.open.qbes.api.http.Constants;
import com.open.qbes.core.annotations.ContextConfiguration;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.concurrent.*;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path(Constants.API_VERSION + "/tests")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@ContextConfiguration
public class QBESClientProxyAPI {

    @POST
    @Path("/{id}")
    public Response receiveResponse(@PathParam("id") String id, Object data) {
        if (id != null && callbacks.containsKey(id)) {
            try {
                CallbackFuture callbackFuture = callbacks.remove(id);
                callbackFuture.result = data;
                callbackFuture.latch.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return Response.ok().build();
    }

    @SuppressWarnings("unchecked")
    public static <T> Future<T> register(String id) {
        CallbackFuture callbackData = new CallbackFuture();
        if (callbacks.putIfAbsent(id, callbackData) != null) {
            throw new IllegalArgumentException("A callback is already registered with id " + id);
        } else {
            return callbackData;
        }
    }

    private static final ConcurrentMap<String, CallbackFuture<Object>> callbacks = new ConcurrentHashMap<>();

    private static class CallbackFuture<T> implements Future<T> {

        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile T result;

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return latch.getCount() == 1;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            latch.await();
            return result;
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            latch.await(timeout, unit);
            return result;
        }
    }
}
