package com.open.qbes.core;

import com.google.common.collect.Sets;
import com.open.qbes.conf.QueueConfig;
import com.open.qbes.core.annotations.ContextConfiguration;
import com.open.qbes.queues.MemoryBasedQueue;
import com.open.utils.Log;

import java.util.*;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorCompletionService;

import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static com.open.qbes.core.JobContext.INHERITED_CACHE;
import static com.open.qbes.core.JobContext.getContext;
import static com.open.qbes.core.annotations.ContextConfiguration.ExecutionState.*;
import static com.open.qbes.core.annotations.ContextConfiguration.Strategy.FAIL_FAST;
import static com.open.utils.StringUtils.asString;
import static java.util.Arrays.asList;

public class GraphJob extends AbstractJob<JobContext> {

    private static final Log log = Log.getLogger(GraphJob.class);

    private final CompletionService<Job> completionService;
    private boolean whenFlag;
    private Job finisher;

    private final Map<Job, Set<Job>> graph = new IdentityHashMap<>();
    private final Set<Job> allJobs = newIdentityHashSet();

    private final JobContext<Object> scopedContext;

    private Job[] whenJobs;

    @SuppressWarnings("unchecked")
    public GraphJob(QueueConfig queueConfig, Map<String, Object> cache, Object contextInfo,
                    ContextConfiguration.Strategy executionStrategy, JobFactory jobFactory) {
        scopedContext = new JobContext(queueConfig, cache, contextInfo, executionStrategy, jobFactory);
        scopedContext.setExecutionState(NOT_STARTED);
        completionService = new ExecutorCompletionService<>(scopedContext.getAssociatedExecutor());
    }

    public GraphJob(ContextConfiguration.Strategy executionStrategy) {
        this(QueueService.getInstance().getDefaultQueueConfig(), INHERITED_CACHE, getContext().getInfo(), executionStrategy, new DefaultJobFactory());
    }

    public GraphJob ifFinished(Job... jobs) {
        ensureNotStarted();
        if (whenFlag)
            throw new IllegalStateException("execute() needs to be called before calling another ifXXX() method");
        whenFlag = true;
        whenJobs = jobs;
        allJobs.addAll(asList(jobs));
        return this;
    }

    public GraphJob IfAnyDone(Job... jobs) {
        ensureNotStarted();
        if (whenFlag)
            throw new IllegalStateException("execute() needs to be called before calling another ifXXX() method");
        whenFlag = true;
        whenJobs = jobs;
        allJobs.addAll(asList(jobs));
        return this;
    }

    @SuppressWarnings("unchecked")
    public GraphJob ifFinished(Class<? extends Job>... jobClasses) throws Exception {
        List<Job> jobs = createJobs(jobClasses);
        return IfAnyDone(jobs.toArray(new Job[jobs.size()]));
    }

    private List<Job> createJobs(Class<? extends Job>[] jobClasses) throws Exception {
        List<Job> jobs = new ArrayList<>();
        for (Class<? extends Job> jobClass : jobClasses) {
            if (scopedContext.getJob(jobClass) != null) {
                Job j = scopedContext.getJob(jobClass);
                jobs.add(j);
            } else {
                Job j = scopedContext.getJobFactory().create(jobClass);
                jobs.add(j);
                scopedContext.jobIdToJob.putIfAbsent(jobClass.getName(), j);
            }
        }
        return jobs;
    }

    @SuppressWarnings("unchecked")
    public GraphJob thenDo(Job... jobs) {
        ensureNotStarted();
        if (!whenFlag) {
            throw new IllegalStateException("One of the whenXXX() methods needs to be called before calling execute()");
        }
        whenFlag = false;

        for (Job job : jobs) {
            if (graph.containsKey(job))
                throw new IllegalArgumentException("Job " + asString(job) + " was added twice for execution");
            Set<Job> set = newIdentityHashSet();
            for (Job whenJob : whenJobs) {
                set.add(whenJob);
            }
            graph.put(job, set);
        }
        allJobs.addAll(asList(jobs));
        return this;
    }

    @SuppressWarnings("unchecked")
    public GraphJob thenDo(Class<? extends Job>... jobClasses) throws Exception {
        List<Job> jobs = createJobs(jobClasses);
        return thenDo(jobs.toArray(new Job[jobs.size()]));
    }

    @SuppressWarnings("unchecked")
    public GraphJob andFinishWith(Job job) {
        this.finisher = job;
        return this;
    }

    @SuppressWarnings("unchecked")
    public GraphJob explain() throws Exception {
        if (allJobs.size() == 0) {
            log.debug("Nothing to explain");
            return this;
        }

        ensureNotStarted();

        // do a deep clone:
        Map<Job, Set<Job>> dupGraph = new IdentityHashMap<>();
        for (Job job : graph.keySet()) {
            Set<Job> jobs = graph.get(job);
            Set<Job> dup = Sets.newIdentityHashSet();
            dup.addAll(jobs);
            dupGraph.put(job, dup);
        }

        Set<Job> dupAllJobs = newIdentityHashSet();
        dupAllJobs.addAll(allJobs);

        Set<Job> dependentJobs = dupGraph.keySet();

        Set<Job> independentJobs = difference(dupAllJobs, dependentJobs);

        Queue<Job> queue = new ConcurrentLinkedQueue<>();

        if (independentJobs.size() == 0) {
            throw new IllegalArgumentException("All jobs are dependent. Task cannot execute");
        }

        List<Job> jobs = new ArrayList<>();
        jobs.addAll(independentJobs);
        Collections.sort(jobs, new Comparator<Job>() {
            @Override
            public int compare(Job o1, Job o2) {
                return o2.priority() - o1.priority();
            }
        });

        log.debug("*************************** Execution Plan *********************");
        for (Job job : jobs) {
            log.debug("*\t(Simulated) Scheduling Job %s", asString(job));
            queue.add(job);
        }

        while (queue.size() > 0) {
            Job finishedJob = queue.remove();
            log.debug("*\t(Simulated) Executing Job %s", asString(finishedJob));
            Iterator<Job> iterator = dupGraph.keySet().iterator();
            List<Job> nextScheduledJobs = new ArrayList<>();
            while (iterator.hasNext()) {
                Job dependentJob = iterator.next();
                Set<Job> whenJobs = dupGraph.get(dependentJob);
                if (whenJobs.contains(finishedJob)) {
                    whenJobs.remove(finishedJob);
                    if (whenJobs.size() == 0) {
                        nextScheduledJobs.add(dependentJob);
                        iterator.remove();
                    }
                }
            }
            if (!nextScheduledJobs.isEmpty()) {
                Collections.sort(nextScheduledJobs, new Comparator<Job>() {
                    @Override
                    public int compare(Job o1, Job o2) {
                        return o2.priority() - o1.priority();
                    }
                });
                log.debug("*\tSome dependency conditions are met. Scheduling next set of jobs");
                for (Job nextScheduledJob : nextScheduledJobs) {
                    log.debug("*\tScheduling Job %s", asString(nextScheduledJob));
                    queue.add(nextScheduledJob);
                }
            }
        }
        if (finisher != null) {
            log.debug("*\t(Simulated) Calling finisher %s", asString(finisher));
        }
        log.debug("*************************** Execution Plan *********************");
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public final JobContext doCall() throws Exception {
        if (allJobs.size() == 0) {
            log.debug("Nothing to execute");
            return scopedContext;
        }

        ensureNotStarted();
        scopedContext.setExecutionState(RUNNING);

        Set<Job> dependentJobs = graph.keySet();

        Set<Job> independentJobs = difference(allJobs, dependentJobs);

        if (independentJobs.size() == 0)
            throw new IllegalArgumentException("All jobs are dependent. Task cannot execute");

        List<Job> jobs = new ArrayList<>();
        jobs.addAll(independentJobs);
        Collections.sort(jobs, new Comparator<Job>() {
            @Override
            public int compare(Job o1, Job o2) {
                return o2.priority() - o1.priority();
            }
        });

        for (Job job : jobs) {
            log.debug("Scheduling Job %s", asString(job));
            submit(job);
        }
        while (scopedContext.getJobCache().size() < allJobs.size()) {
            MemoryBasedQueue.MBQFutureTask future = (MemoryBasedQueue.MBQFutureTask) completionService.take();
            Job finishedJob = ((JobContext.DecoratedJob) future.getCallable()).inner;

            log.debug("Job finished %s ", finishedJob);

            Object result = scopedContext.getResult(finishedJob.getId());

            if (result instanceof Throwable) {
                scopedContext.setExecutionState(FINISHED_WITH_ERRORS);
                if (scopedContext.getStartingStrategy() == FAIL_FAST) {
                    log.error("Job %s has execution error. Terminating the entire execution (strategy = FAIL_FAST)", (Throwable) result, finishedJob);
                    if (finisher != null) {
                        log.debug("Executing finisher %s", asString(finisher));
                        scopedContext.decorate(finisher, scopedContext).call();
                    }
                    return scopedContext;
                }
            }
            Iterator<Job> iterator = graph.keySet().iterator();
            List<Job> nextScheduledJobs = new ArrayList<>();
            while (iterator.hasNext()) {
                Job dependentJob = iterator.next();
                Set<Job> whenJobs = graph.get(dependentJob);
                if (whenJobs.contains(finishedJob)) {
                    whenJobs.remove(finishedJob);
                    if (whenJobs.size() == 0) {
                        nextScheduledJobs.add(dependentJob);
                        iterator.remove();
                    }
                }
            }
            if (!nextScheduledJobs.isEmpty()) {
                Collections.sort(nextScheduledJobs, new Comparator<Job>() {
                    @Override
                    public int compare(Job o1, Job o2) {
                        return o2.priority() - o1.priority();
                    }
                });
                log.debug("Some dependency conditions are met. Scheduling next set of jobs");
                for (Job nextScheduledJob : nextScheduledJobs) {
                    log.debug("Scheduling Job %s", asString(nextScheduledJob));
                    submit(nextScheduledJob);
                }
            }
        }
        if (scopedContext.getExecutionState() == RUNNING) {
            scopedContext.setExecutionState(FINISHED_NORMALLY);
        }
        if (finisher != null) {
            log.debug("Executing finisher %s", asString(finisher));
            scopedContext.decorate(finisher, scopedContext).call();
        }
        return scopedContext;
    }

    private void ensureNotStarted() {
        if (scopedContext.getExecutionState() != NOT_STARTED) {
            throw new IllegalStateException("Execution not started yet");
        }
    }

    @SuppressWarnings("unchecked")
    private void submit(Job job) {
        completionService.submit((Job) scopedContext.decorate(job));
    }
}
