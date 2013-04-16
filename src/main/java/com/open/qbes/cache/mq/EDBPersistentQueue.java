package com.open.qbes.cache.mq;

import com.google.gson.Gson;
import com.open.qbes.cache.JobCache;
import com.open.qbes.conf.QueueConfig;
import com.open.qbes.core.Job;
import com.open.qbes.persistence.DB;
import com.open.utils.Log;
import com.open.qbes.core.JobContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.open.qbes.persistence.DB.*;

public class EDBPersistentQueue implements JobCache {

    private static final Log log = Log.getLogger(EDBPersistentQueue.class);

    private static final String CREATE_TABLE =
            "CREATE TABLE @table" +
                    "(" +
                    "  id BIGINT AUTO_INCREMENT NOT NULL," +
                    "  job_data VARCHAR(1000)," +
                    "  created_at TIMESTAMP NOT NULL DEFAULT now()," +
                    "  PRIMARY KEY (id)" +
                    ");";

    private String table;
    private String context;
    private final Gson gson = new Gson();
    private long currentIdValue;

    @Override
    public void init(QueueConfig queueConfig) {
        context = System.getProperty("rl.db");
        log.info("Using %s as db context", context);

        // create the jobs table here:
        table = "qbes__" + queueConfig.getQueueId().replaceAll("-", "_") + "__queue";
        String createQuery = CREATE_TABLE.replaceAll("@table", table);
        try {
            DB.DB_CONTEXT.set(context);
            try {
                update(createQuery);
                log.info("Created new jobs table %s", table);
            } catch (Exception e) {
                if (e.getMessage() != null && e.getMessage().contains("exists")) {
                    // we're fine, the table exists:
                    log.info(table + " already exists. No need to create.");
                } else throw new RuntimeException(e.getMessage());
            }

            log.info("Loading the initial id count");
            long id = getId(DB.fetch("SELECT min(id) id FROM " + table));
            if (id > 0) {
                currentIdValue = id;
                log.debug("Initial id value is set to %d", currentIdValue);
            } else {
                log.debug("No data in the table, initial id set to 0");
            }
        } finally {
            DB.DB_CONTEXT.remove();
        }
    }

    private long getId(List<Map<String, Object>> data) {
        if (data == null || data.size() == 0 || data.get(0).get("ID") == null)
            return -1;
        return ((Number) data.get(0).get("ID")).longValue();
    }

    @Override
    public boolean isEmpty() {
        try {
            DB.DB_CONTEXT.set(context);
            return DB.fetch("SELECT 1 FROM " + table + " LIMIT 1").size() == 0;
        } finally {
            DB.DB_CONTEXT.remove();
        }
    }

    @Override
    public int getInitialJobCount() {
        try {
            DB.DB_CONTEXT.set(context);
            int initialCount = ((Number) DB.fetch("SELECT count(*) size FROM " + table).get(0).get("SIZE")).intValue();
            log.debug("Setting initial count as %d", initialCount);
            return initialCount;
        } finally {
            DB.DB_CONTEXT.remove();
        }
    }

    @Override
    public void suspend() {
        //no-op
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public synchronized void put(Job job) throws Exception {
        log.debug("Adding job data %s", JobContext.getContext().getInitialJobData());
        String data = gson.toJson(JobContext.getContext().getInitialJobData());
        try {
            DB.DB_CONTEXT.set(context);
            update("INSERT INTO " + table + "(job_data) VALUES(?)", data);
        } finally {
            DB.DB_CONTEXT.remove();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized Job get() throws Exception {
        try {
            DB.DB_CONTEXT.set(context);
            //TODO: may be optimized more:
            List<Map<String, Object>> row = fetch("SELECT id, job_data FROM " + table + " WHERE id >= ? ORDER BY id LIMIT 1", currentIdValue);
            long id = getId(row);
            if (id == -1 || currentIdValue == id) {
                return null;
            } else {
                currentIdValue = id;
                String data = row.get(0).get("JOB_DATA").toString();
                Map<String, Object> jobData = gson.fromJson(data, HashMap.class);
                int deleteCount = update("DELETE FROM " + table + " WHERE id = ?", currentIdValue);
                if (deleteCount != 1)
                    log.error("Error in deleting the last retrieved id %d", currentIdValue);
                log.debug("Retrieved job data %s", jobData);
                return null;
            }
        } finally {
            DB.DB_CONTEXT.remove();
        }
    }

    @Override
    public void shutdown() {
        //no-op
    }
}
