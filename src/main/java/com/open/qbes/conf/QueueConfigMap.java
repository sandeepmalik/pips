package com.open.qbes.conf;

import com.open.qbes.queues.Queue;
import com.open.utils.Log;

import java.util.HashMap;
import java.util.Map;

public class QueueConfigMap extends HashMap<String, Object> implements QueueConfig {

    private Queue queue;
    private String queueId;
    private volatile boolean setOnce;
    private static final Log log = Log.getLogger(QueueConfig.class);

    private QueueConfigMap() {
    }

    public static QueueConfig buildFrom(Map<String, Object> jsonData) {
        QueueConfigMap queueConfigMap = new QueueConfigMap();
        if (!jsonData.containsKey("queue_id"))
            throw new IllegalArgumentException("No queue_id present");
        queueConfigMap.putAll(jsonData);
        queueConfigMap.queueId = jsonData.get("queue_id").toString();
        log.debug("Built config for " + queueConfigMap.getQueueId());
        return queueConfigMap;
    }

    @Override
    public String getQueueId() {
        return queueId;
    }

    @Override
    public Queue getAssociatedQueue() {
        return queue;
    }

    @Override
    public void setOneTimeAssociatedQueue(Queue queue) {
        if (!setOnce) {
            this.queue = queue;
            setOnce = true;
        } else throw new UnsupportedOperationException("Queue already been set");
    }

    @Override
    public Object get(Object key) {
        if (!containsKey(key))
            throw new NullPointerException("No config data for " + key);
        return super.get(key);
    }

    @Override
    public String type() {
        return get("queue_type").toString();
    }

    @Override
    public String cacheType() {
        return get("cache", "memory").toString();
    }

    @Override
    public String jobType() {
        return get("job_type").toString();
    }

    @Override
    public String mode() {
        return get("mode", "concurrent").toString();
    }

    @Override
    public <V> V get(String key, V defaultVal) {
        return containsKey(key) ? (V) super.get(key) : defaultVal;
    }

    @Override
    public void set(String key, Object value) {
        put(key, value);
    }

    @Override
    public boolean has(String key) {
        return containsKey(key);
    }

    public void update(QueueConfig newConfig) {
        QueueConfigMap map = (QueueConfigMap) newConfig;
        for (String s : map.keySet()) {
            put(s, map.get(s));
        }
    }
}
