package com.open.qbes.core;

import com.google.common.base.Splitter;
import com.open.fixtures.TestContext;
import com.open.qbes.conf.QueueConfig;
import com.open.qbes.core.annotations.EnqueueRootElement;
import com.open.qbes.core.annotations.RunsOnInit;
import com.open.qbes.persistence.DB;
import com.open.qbes.queues.MemoryBasedQueue;
import com.open.qbes.queues.Queue;
import com.open.qbes.queues.QueueStats;
import com.open.qbes.queues.RateLimitedQueue;
import com.open.utils.JSONUtils;
import com.open.utils.Log;
import com.startup.StartupOptions;
import org.reflections.Reflections;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;

import static com.open.qbes.conf.QueueConfigMap.buildFrom;
import static com.open.qbes.core.BlockingJob.AWAIT;
import static com.open.qbes.core.BlockingJob.UNITS;
import static com.open.utils.JSONUtils.map;
import static com.open.utils.Pair.pair;
import static com.open.utils.StringUtils.asString;
import static java.lang.String.format;
import static java.lang.System.getProperty;

public class QueueService {

    public static final String QUEUE_ID = "queue_id";
    public static final String JOB_TYPE = "job_type";
    private static QueueService ourInstance = new QueueService();

    private static final Log log = Log.getLogger(QueueService.class);

    public static final String DEFAULT_QUEUE = "DEFAULT";

    private Properties config;

    public static QueueService getInstance() {
        return ourInstance;
    }

    private final ConcurrentMap<String, QueueConfig> queues = new ConcurrentHashMap<>();
    private volatile QueueConfig defaultQueueConfig;
    private final Map<String, Class<? extends Job>> jobs = new HashMap<>();

    private ConcurrentMap<SystemShutdownListener, Boolean> shutdownListeners = new ConcurrentHashMap<>();
    private ConcurrentLinkedQueue<ServiceStatusResponder> serviceStatusResponders = new ConcurrentLinkedQueue<>();

    private QueueService() {
    }

    public synchronized Properties initProperties() throws Exception {
        if (config != null)
            return config;
        log.info("Loading config properties");
        config = new Properties();

        InputStream inputStream;
        String module = getProperty("resources");

        if (TestContext.isRunningTests()) {
            log.info("Trying to load the %s config if present.", module + ".test.qbes");
            try {
                inputStream = getResource(module + ".test.qbes");
            } catch (Exception ignore) {
                log.warn("Ideally for test environment %s should be present. Loading %s instead", module + ".test.qbes", module + ".qbes");
                inputStream = getResource(module + ".qbes");
            }
        } else {
            inputStream = getResource(module + ".qbes");
        }

        config.load(inputStream);
        for (Object o : config.keySet()) {
            String str = o.toString();
            if (str.startsWith("system.")) {
                String systemProp = str.substring("system.".length());
                if (System.getProperty(systemProp) != null) {
                    if (!systemProp.contains("password"))
                        log.info("%s is already set as %s, skipping value from config file", systemProp, System.getProperty(systemProp));
                    else
                        log.info("%s is already set as *****, skipping value from config file", systemProp);
                } else {
                    if (!systemProp.contains("password"))
                        log.info("System property set: " + systemProp + " =  " + config.getProperty(str));
                    else log.info("System property set: " + systemProp + " =  *****");
                    System.setProperty(systemProp, config.getProperty(str));
                }
            }
        }
        return config;
    }

    @SuppressWarnings("unchecked")
    public void init() throws Exception {
        createDefaultQueue();
        log.info("Creating preconfigured queues, if any");
        createPreconfiguredQueues();
    }

    @SuppressWarnings("unchecked")
    public void runInitJobs() throws Exception {
        log.info("Starting the init-jobs, if any");
        List<Job> blockInitJobs = new ArrayList<>();
        List<Job> asyncInitJobs = new ArrayList<>();
        for (String key : jobs.keySet()) {
            Class<? extends Job> jobClass = jobs.get(key);
            if (jobClass.isAnnotationPresent(RunsOnInit.class)) {
                String queueId = jobClass.getAnnotation(RunsOnInit.class).queue();
                if (queueId == DEFAULT_QUEUE) {
                    log.warn("No queue associated with job %s, Will be run in default queue on start up", jobClass.getSimpleName());
                }
                try {
                    Job<?> job = jobClass.newInstance();
                    if (jobClass.getAnnotation(RunsOnInit.class).blocks())
                        blockInitJobs.add(job);
                    else asyncInitJobs.add(job);
                } catch (Exception e) {
                    log.error("Could not run job %s  on startup", jobClass);
                    throw e;
                }
            }
        }
        if (asyncInitJobs.size() > 0) {
            log.info("Invoking async init jobs");
            for (Job asyncInitJob : asyncInitJobs) {
                Job job = filterBasedOnModule(asyncInitJob);
                if (job != null) {
                    log.debug("Enqueuing async init job %s", job);
                    JobContext.getContext().enqueue(job);
                }
            }
        }
        if (blockInitJobs.size() > 0) {
            List<Job> blockingJobs = new ArrayList<>();
            for (Job blockInitJob : blockInitJobs) {
                Job job = filterBasedOnModule(blockInitJob);
                if (job != null) {
                    log.info("Invoking blocking jobs");
                    blockingJobs.add(blockInitJob);
                }
            }
            JobContext.getContext().parallel(blockingJobs.toArray(new Job[blockingJobs.size()]));
        }
    }

    private Job filterBasedOnModule(Job initJob) throws Exception {
        String[] modules = initJob.getClass().getAnnotation(RunsOnInit.class).modules();
        for (String module : modules) {
            if (module != null && module.equals(StartupOptions.resources)) {
                if (initJob.getClass().getAnnotation(RunsOnInit.class).runsInTestModeOnly()) {
                    if (TestContext.isRunningTests()) {
                        return initJob;
                    } else return null;
                } else {
                    return initJob;
                }
            }
        }
        return null;
    }

    private void createPreconfiguredQueues() throws Exception {
        try {
            // see if any queues.xml is defined:
            getResource("queues.xml").close();
        } catch (Exception ignore) {
            log.info("No pre configured queues found");
            return;
        }
        Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(getResource("queues.xml"));
        NodeList nl = document.getElementsByTagName("queue");
        for (int i = 0; i < nl.getLength(); i++) {
            Element e = (Element) nl.item(i);
            Map<String, Object> queueData = new HashMap<>();
            NamedNodeMap namedNodeMap = e.getAttributes();
            for (int j = 0; j < namedNodeMap.getLength(); j++) {
                Attr attribute = (Attr) namedNodeMap.item(j);
                queueData.put(attribute.getName(), attribute.getValue());
            }
            log.info("Starting new queue with attributes " + queueData);
            QueueConfig queueConfig = buildFrom(queueData);
            create(queueConfig);
        }
    }

    @SuppressWarnings("unchecked")
    public Queue create(QueueConfig queueConfig) {
        // create a new queue:
        QueueConfig config = queues.putIfAbsent(queueConfig.getQueueId(), queueConfig);
        if (config != null) {
            throw new IllegalStateException("Queue Already Exists with id " + queueConfig.getQueueId());
        } else {
            // dp the real queue creation:
            String type = queueConfig.type();
            if ("mbq".equalsIgnoreCase(type)) {
                MemoryBasedQueue mbq = new MemoryBasedQueue();
                queueConfig.setOneTimeAssociatedQueue(mbq);
                mbq.start(queueConfig);
            } else if ("rlq".equalsIgnoreCase(type)) {
                RateLimitedQueue rlq = new RateLimitedQueue(new MemoryBasedQueue());
                queueConfig.setOneTimeAssociatedQueue(rlq);
                rlq.start(queueConfig);
            } else {
                throw new IllegalArgumentException("Unknown queue type " + type);
            }
        }
        return queueConfig.getAssociatedQueue();
    }

    public QueueStats suspend(String queueId) {
        if (queues.containsKey(queueId)) {
            queues.get(queueId).getAssociatedQueue().suspend();
            return queues.get(queueId).getAssociatedQueue().stats();
        } else throw new IllegalStateException("Queue doesn't exist for id " + queueId);
    }

    @SuppressWarnings("unchecked")
    public void reloadMappings() throws Exception {
        if (System.getProperty("qbes.jobs.packages") != null) {
            String packages = System.getProperty("qbes.jobs.packages").trim();
            String[] parts = packages.split(",");
            for (String part : parts) {
                log.info("Loading jobs from package %s", part);
                Reflections reflections = new Reflections(part);
                Set<Class<?>> jobClasses = reflections.getTypesAnnotatedWith(EnqueueRootElement.class);
                for (Class<?> jobClass : jobClasses) {
                    Class<? extends Job> dup = (Class<? extends Job>) jobClass;
                    EnqueueRootElement enqueueRootElement = jobClass.getAnnotation(EnqueueRootElement.class);
                    if (EnqueueRootElement.USE_UNQUALIFIED_CLASS_NAME.equals(enqueueRootElement.value())) {
                        addJobProcessor(jobClass.getSimpleName(), dup);
                    } else {
                        Iterator<String> values = Splitter.on(",").omitEmptyStrings().trimResults().split(enqueueRootElement.value()).iterator();
                        while (values.hasNext()) {
                            addJobProcessor(values.next(), dup);
                        }
                    }
                }
            }
        }
        log.info("Reloaded jobs mapping");
    }

    public static InputStream getResource(String name) throws Exception {
        InputStream inputStream = QueueService.getInstance().getClass().getResourceAsStream(name);
        if (inputStream != null)
            return inputStream;
        else if (System.getProperty("shared.loader") != null)
            return new FileInputStream(System.getProperty("shared.loader") + File.separator + name);
        else throw new Exception(String.format("resource %s not found", name));
    }

    @SuppressWarnings("unchecked")
    public Queue update(QueueConfig queueConfig) {
        if (queues.containsKey(queueConfig.getQueueId())) {
            queues.get(queueConfig.getQueueId()).getAssociatedQueue().update(queueConfig);
            return queues.get(queueConfig.getQueueId()).getAssociatedQueue();
        }
        return null;
    }

    public QueueStats delete(String queueId) {
        if (queues.containsKey(queueId)) {
            log.info("Deleting queue " + queueId);
            return queues.remove(queueId).getAssociatedQueue().destroy();
        } else throw new IllegalArgumentException("No queue to delete with id " + queueId);
    }

    public QueueStats stats(String queueId) {
        if (queues.containsKey(queueId)) {
            return queues.get(queueId).getAssociatedQueue().stats();
        } else throw new IllegalStateException("No queue found for id " + queueId);
    }

    public QueueStats[] stats(String... queueIds) {
        List<QueueStats> stats = new ArrayList<>();
        if (queueIds.length == 0) {
            for (String queueId : queues.keySet()) {
                stats.add(stats(queueId));
            }
        } else {
            for (String queueId : queueIds) {
                stats.add(stats(queueId));
            }
        }
        return stats.toArray(new QueueStats[stats.size()]);
    }

    public QueueConfig getQueueConfig(String queueId) {
        return queues.get(queueId);
    }

    public QueueConfig getDefaultQueueConfig() {
        return defaultQueueConfig;
    }

    @SuppressWarnings("unchecked")
    <T> T enqueue(Queue queue, Job job) throws Exception {
        if (job.getClass().isAnnotationPresent(BlockingJob.class)) {
            return (T) processBlockingJob(queue, job);
        } else {
            // async enqueue:
            return (T) queue.enqueue(job);
        }
    }

    private <T> T processBlockingJob(Queue queue, Job<T> job) throws Exception {
        if (JobContext.getContext().contains(AWAIT) && JobContext.getContext().contains(UNITS)) {
            long await = JSONUtils.doubleToLong(JobContext.getContext().get(AWAIT), Long.MIN_VALUE);
            if (await == Long.MIN_VALUE)
                throw new IllegalArgumentException("No value or 'unparseable' set for await");
            TimeUnit timeUnit = TimeUnit.valueOf(JobContext.getContext().get(UNITS).toString());
            log.debug("%s is a blocking job", asString(job));
            Future<T> future = queue.enqueue(job);
            // await for the result to complete:
            T result;
            try {
                if (await == -1) {
                    log.debug("Awaiting indefinitely for the %s job to complete", asString(job));
                    // wait looooooong:
                    result = future.get();
                } else {
                    log.debug("Awaiting for the job %s, for %d %s", asString(job), await, timeUnit);
                    result = future.get(await, timeUnit);
                }
            } catch (TimeoutException e) {
                log.error("Job %s did not complete in stipulated time", e, asString(e));
                throw e;
            } catch (Exception e) {
                log.error("Error occurred in execution of %s", e, asString(job));
                throw e;
            }
            log.debug("Returning result for %s", asString(job));
            return result;
        } else
            throw new IllegalArgumentException(format("jobData for %s needs to contain 'await' time and 'units' values (e.g. MILLISECONDS, SECONDS, MINUTES, etc)", asString(job)));
    }

    @SuppressWarnings("unchecked")
    public Job<Object> createJob(QueueConfig config, Map<String, Object> jobData) {
        String jobType;
        if (jobData.containsKey(JOB_TYPE)) {
            jobType = jobData.get(JOB_TYPE).toString();
        } else {
            jobType = config.jobType();
        }
        if (!jobs.containsKey(jobType))
            throw new IllegalArgumentException("No job processor found for job type " + jobType);
        try {
            Job job = jobs.get(jobType).newInstance();
            return job;
        } catch (Exception e) {
            log.error("Error in creating a job ", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public synchronized void addJobProcessor(String jobType, Class<? extends Job> jobProcessorClass) {
        log.info(jobType + " => " + jobProcessorClass);
        jobs.put(jobType, jobProcessorClass);
    }

    public QueueStats[] shutdown() {
        log.info("Shutting down system shutdown listeners");
        for (SystemShutdownListener listener : shutdownListeners.keySet()) {
            listener.shutdown();
            log.info("%s is shutdown", listener);
        }
        List<QueueStats> stats = new ArrayList<>();
        for (String queueId : queues.keySet()) {
            stats.add(queues.get(queueId).getAssociatedQueue().destroy());
        }
        DB.shutdown();
        return stats.toArray(new QueueStats[stats.size()]);
    }

    public List<Map<String, String>> halt() {
        log.info("Halting system shutdown listeners");
        for (SystemShutdownListener listener : shutdownListeners.keySet()) {
            listener.halt();
            log.info("%s is halted", listener);
        }
        List<Map<String, String>> haltStatus = new ArrayList<>();
        for (String queueId : queues.keySet()) {
            log.info("Suspending queue " + queueId);
            queues.get(queueId).getAssociatedQueue().suspend();
            Map<String, String> status = new HashMap<>();
            status.put(queueId, "Suspended");
            haltStatus.add(status);
        }
        log.info("Halting the DB interface");
        DB.shutdown();
        return haltStatus;
    }

    public void addShutdownListener(SystemShutdownListener listener) {
        shutdownListeners.putIfAbsent(listener, true);
    }

    public void addServiceStatusResponder(ServiceStatusResponder responder) {
        serviceStatusResponders.add(responder);
    }

    public List<ServiceStatusResponder.ServiceStatus> getServiceStatus() {
        List<ServiceStatusResponder.ServiceStatus> statuses = new ArrayList<>();
        for (ServiceStatusResponder serviceStatusResponder : serviceStatusResponders) {
            statuses.add(serviceStatusResponder.getStatus());
        }
        return statuses;
    }

    private Queue createDefaultQueue() {
        log.info("Creating system's default queue");
        defaultQueueConfig = buildFrom(map(pair("default", "true"), pair("queue_type", "mbq"), pair("cache", "memory"), pair("mode", "concurrent"), pair("queue_id", DEFAULT_QUEUE)));
        return create(defaultQueueConfig);
    }
}
