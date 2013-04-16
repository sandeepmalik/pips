package com.open.qbes.core;

import com.open.qbes.QBES;
import com.open.qbes.api.http.StatusCode;
import com.open.qbes.conf.QueueConfig;
import com.open.qbes.core.annotations.ContextConfiguration;
import com.open.qbes.queues.Queue;
import com.open.utils.ExceptionUtils;
import com.open.utils.Pair;
import com.open.utils.Tuple;
import com.startup.StartupOptions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.open.utils.Pair.pair;
import static java.util.Collections.unmodifiableMap;

public class JobContext<T> implements RequiresContextualLogInfo<T>, ExecutionContext<T> {

    private final Queue queue;
    private final Map<String, Object> dataCache;
    private final Map<String, Object> unalteredCache;

    ConcurrentMap<String, Job> jobIdToJob = new ConcurrentHashMap<>();
    private final ConcurrentMap<Job, JobResult> jobCache = new ConcurrentHashMap<>();

    private T contextInfo;
    private final JobFactory jobFactory;

    private final ContextConfiguration.Strategy startingStrategy;
    private volatile ContextConfiguration.Strategy finishingStrategy;
    private volatile ContextConfiguration.ExecutionState executionState;

    private volatile StatusCode statusCode = StatusCode.INTERNAL_SERVER_ERROR;

    private final ExecutionContext<Object> parent;

    public static final Map<String, Object> INHERITED_CACHE = unmodifiableMap(new HashMap<String, Object>());

    @SuppressWarnings("unchecked")
    public JobContext(QueueConfig queueConfig, Map<String, Object> dataCache, T contextInfo,
                      ContextConfiguration.Strategy startingStrategy, JobFactory jobFactory) {
        if (getContext() != null)
            this.parent = getContext();
        else if (this != QBES.INIT_CONTEXT)
            this.parent = QBES.INIT_CONTEXT;
        else this.parent = null;

        if (dataCache == INHERITED_CACHE) {
            if (this.parent != null)
                this.dataCache = ((JobContext) this.parent).dataCache;
            else this.dataCache = new ConcurrentHashMap<>();
        } else {
            this.dataCache = dataCache;
        }

        if (contextInfo != null)
            setContextInfo(contextInfo);
        else if (parent != null)
            setContextInfo((T) parent.getInfo());

        this.queue = queueConfig.getAssociatedQueue();

        if (this.parent != null && this.parent != QBES.INIT_CONTEXT)
            unalteredCache = ((JobContext) this.parent).unalteredCache;
        else
            unalteredCache = unmodifiableMap(new HashMap<>(dataCache));

        this.startingStrategy = startingStrategy;
        this.jobFactory = jobFactory;
    }

    protected Map<String, Object> getCache() {
        return unmodifiableMap(dataCache);
    }

    @Override
    public void setContextInfo(T contextInfo) {
        this.contextInfo = contextInfo;
    }

    @Override
    public Map<String, Object> getInitialJobData() {
        return unalteredCache;
    }

    @SuppressWarnings("unchecked")
    public static <T> ExecutionContext<T> getContext() {
        return CONTEXT.get();
    }

    public static void setContext(ExecutionContext context) {
        CONTEXT.set(context);
    }

    public static void removeContext() {
        CONTEXT.remove();
    }

    private static ThreadLocal<ExecutionContext> CONTEXT = new ThreadLocal<>();

    @Override
    public T getInfo() {
        return contextInfo;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T enqueue(Job<T> job) throws Exception {
        job.validate(getCache());
        return QueueService.getInstance().enqueue(queue, decorate((Job) job));
    }

    @Override
    public Job<T> decorate(Job<T> job) {
        return decorate(job, this);
    }

    @Override
    public Job<T> decorate(Job<T> job, ExecutionContext jobContext) {
        if (job instanceof DecoratedJob) {
            return job;
        }
        return new DecoratedJob<>(job, (JobContext) jobContext);
    }

    static class DecoratedJob<T> implements Job<T> {

        final Job<T> inner;
        final JobContext callerContext;

        private DecoratedJob(Job<T> inner, JobContext callerContext) {
            this.inner = inner;
            this.callerContext = callerContext;
        }

        @Override
        public String getId() {
            return inner.getId();
        }

        @Override
        public int priority() {
            return inner.priority();
        }

        @Override
        public void validate(Map<String, Object> jobData) {
            inner.validate(jobData);
        }

        @Override
        public void onComplete(JobCallback callbackJob) {
            inner.onComplete(callbackJob);
        }

        @Override
        @SuppressWarnings("unchecked")
        public T call() throws Exception {
            try {
                setContext(callerContext);
                callerContext.jobIdToJob.put(inner.getId(), inner);
                T result = inner.call();
                callerContext.jobCache.put(inner, new JobResult(inner, result));
                return result;
            } catch (Throwable e) {
                callerContext.jobCache.put(inner, new JobResult(inner, e));
                throw e;
            } finally {
                removeContext();
            }
        }

        public int compareTo(Job<T> o) {
            return inner.compareTo(o);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T enqueue(Class<Job> jobClass) throws Exception {
        Job<T> j = jobClass.newInstance();
        jobIdToJob.put(j.getId(), j);
        return enqueue(j);
    }

    public CompletionService<?> enqueue(Job... jobs) {
        if (true)
            throw new UnsupportedOperationException("Yet to be implemented");
        for (Job job : jobs) {
            job.validate(getCache());
        }
        return queue.enqueue(jobs);
    }

    @Override
    public boolean isFinished(String id) {
        return jobIdToJob.containsKey(id) && jobCache.containsKey(jobIdToJob.get(id));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getResult(String id) {
        if (!isFinished(id))
            throw new IllegalStateException("Job " + jobIdToJob.get(id) + " not finished yet");
        return (T) jobCache.get(jobIdToJob.get(id)).result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getResult(Job job) {
        return (T) jobCache.get(job).result;
    }

    @Override
    public ExecutionResult getExecutionResult() {
        //TODO:
        return null;
    }

    @Override
    public Map<Job, Object> getFinishedJobs() {
        Map<Job, Object> finishedJobs = new HashMap<>();
        for (JobResult jobResult : jobCache.values()) {
            finishedJobs.put(jobResult.job, jobResult.result);
        }
        return finishedJobs;
    }

    @Override
    public ContextConfiguration.Strategy getStartingStrategy() {
        return startingStrategy;
    }

    @Override
    public ContextConfiguration.ExecutionState getExecutionState() {
        return executionState;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <J extends Job> J getJob(Class<J> jobClass) {
        return (J) jobIdToJob.get(jobClass.getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <J extends Job> J getJob(String id) {
        return (J) jobIdToJob.get(id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T put(String key, T value) {
        return (T) dataCache.put(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T putIfAbsent(String key, T value) {
        if (dataCache instanceof ConcurrentMap) {
            return (T) ((ConcurrentMap<String, Object>) dataCache).putIfAbsent(key, value);
        } else {
            return put(key, value);
        }
    }

    @Override
    public Map<String, Object> cloneData() {
        return new HashMap<>(dataCache);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void put(Map data) {
        dataCache.putAll(data);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T remove(String key) {
        return (T) dataCache.remove(key);
    }

    private static final Object NOT_FOUND = new Object();

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        Object value = get0(key);
        return value == NOT_FOUND ? null : (T) value;
    }

    @SuppressWarnings("unchecked")
    public Object get0(String key) {
        if (dataCache.containsKey(key))
            return dataCache.get(key);
        JobContext context = this;
        while (context.parent != null) {
            context = (JobContext) context.parent;
            if (context.dataCache.containsKey(key))
                return context.dataCache.get(key);
            else continue;
        }
        return NOT_FOUND;
    }

    @Override
    public boolean contains(String key) {
        return get0(key) != NOT_FOUND;
    }

    public Executor getAssociatedExecutor() {
        return queue.getAssociatedExecutor();
    }

    /*public String getErrorsReport() {
        throw new UnsupportedOperationException();
        *//*if (errorCount() == 0)
            return "";
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("\nError Report\n");
        stringBuilder.append("Context: ").append(getInfo()).append("\n");
        for (ErrorReportElement errorReportElement : errorReportElements) {
            stringBuilder.append("Thread: ").append(errorReportElement.threadName)
                    .append("\t").append("Message: ").append(errorReportElement.errorMessage)
                    .append("\t").append("Stack: ").append(errorReportElement.stack)
                    .append("\n");
            stringBuilder.append(errorReportElement).append("\n");
        }
        return concatenateErrors(stringBuilder.toString());*//*
    }*/

    public static String concatenateErrors(String errorStack) {
        if (!StartupOptions.concatenateErrors || StartupOptions.errorsJoiner.equals("N_L"))
            return errorStack.toString();
        else
            return errorStack.toString().replace("\n", StartupOptions.errorsJoiner);
    }

    public static class JobResult {
        private final Job job;
        private final Object result;

        public JobResult(Job job, Object result) {
            this.job = job;
            this.result = result;
        }

        public Job getJob() {
            return job;
        }

        public Object getResult() {
            return result;
        }

        public boolean isError() {
            return result instanceof Throwable;
        }
    }

    @Override
    public JobFactory getJobFactory() {
        return jobFactory;
    }

    public void setFinishingStrategy(ContextConfiguration.Strategy finishingStrategy) {
        this.finishingStrategy = finishingStrategy;
    }

    @Override
    public ContextConfiguration.Strategy getFinishingStrategy() {
        return finishingStrategy;
    }

    public void setExecutionState(ContextConfiguration.ExecutionState executionState) {
        this.executionState = executionState;
    }

    public ConcurrentMap<Job, JobResult> getJobCache() {
        return jobCache;
    }

    @Override
    public <T> T get(String key, Class<T> valueType) {
        return valueType.cast(get(key));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T1, T2, T3> Tuple<T1, T2, T3> parallel(Job<T1> job1, Job<T2> job2, Job<T3> job3) throws Exception {
        List<Object> results = parallel0(new Job[]{job1, job2, job3});
        return new Tuple<>((T1) results.get(0), (T2) results.get(1), (T3) results.get(2));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T1, T2> Pair<T1, T2> parallel(Job<T1> job1, Job<T2> job2) throws Exception {
        List<Object> results = parallel0(new Job[]{job1, job2});
        return pair((T1) results.get(0), (T2) results.get(1));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> List<T> parallel(Job... jobs) throws Exception {
        return (List<T>) parallel0(jobs);
    }

    private List<Object> parallel0(Job[] jobs) throws Exception {
        Pair<Boolean, List<Object>> pair = parallelExecuteWithStrategy(ContextConfiguration.Strategy.EXECUTE_ALL, jobs);
        if (pair.getItem1())
            throw new Exception("Exception in parallel execute");
        return pair.getItem2();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Pair<Boolean, List<Object>> parallelGet(Job... jobs) throws Exception {
        return parallelExecuteWithStrategy(ContextConfiguration.Strategy.EXECUTE_ALL, jobs);
    }

    private Pair<Boolean, List<Object>> parallelExecuteWithStrategy(ContextConfiguration.Strategy strategy, Job[] jobs) throws Exception {
        GraphJob graphJob = new GraphJob(strategy);
        JobContext graphContext = graphJob.ifFinished(jobs).explain().call();
        Map<Job, Object> results = graphContext.getFinishedJobs();
        boolean hasErrors = false;
        List<Object> resultObjects = new ArrayList<>();
        for (Job job : jobs) {
            Object result = results.get(job);
            if (result instanceof Throwable) {
                hasErrors = true;
            }
            resultObjects.add(result);
        }
        return pair(hasErrors, resultObjects);
    }

    @Override
    public ExecutionContext getSuper() {
        return parent;
    }

    private static class ErrorReportElement implements Serializable {
        private final String errorMessage;
        private final String stack;
        private final String threadName;

        private ErrorReportElement(Throwable throwable) {
            this("None", throwable);
        }

        private ErrorReportElement(String errorMessage, Throwable throwable) {
            this.errorMessage = errorMessage;
            if (throwable != null)
                this.stack = ExceptionUtils.getExceptionString(throwable);
            else stack = "";
            threadName = Thread.currentThread().getName();
        }
    }
}
