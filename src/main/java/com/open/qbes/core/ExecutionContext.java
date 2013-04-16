package com.open.qbes.core;

import com.open.qbes.core.annotations.ContextConfiguration;
import com.open.utils.Pair;
import com.open.utils.Tuple;

import java.util.List;
import java.util.Map;

/**
 * Created by smalik
 * User: Sandeep Malik
 * Date: 12/7/12
 * Time: 4:28 PM
 */
public interface ExecutionContext<T> extends RequiresContextualLogInfo<T> {

    /*
        Job Cache Methods:
     */
    boolean isFinished(String id);

    <T> T getResult(String id);

    <T> T getResult(Job job);

    <J extends Job> J getJob(Class<J> jobClass);

    <J extends Job> J getJob(String id);

    Map<Job, Object> getFinishedJobs();

    /*
        Execution Context Methods:
     */
    ExecutionResult getExecutionResult();

    ContextConfiguration.Strategy getStartingStrategy();

    ContextConfiguration.Strategy getFinishingStrategy();

    ContextConfiguration.ExecutionState getExecutionState();

    ExecutionContext getSuper();

    JobFactory getJobFactory();

    /*
        Data Cache Methods:
     */
    <T> T put(String key, T value);

    <T> T putIfAbsent(String key, T value);

    void put(Map data);

    boolean contains(String key);

    <T> T get(String key);

    T remove(String key);

    Map<String, Object> cloneData();

    Map<String, Object> getInitialJobData();

    /*
        Utility methods:
     */
    <T1, T2, T3> Tuple<T1, T2, T3> parallel(Job<T1> job1, Job<T2> job2, Job<T3> job3) throws Exception;

    <T1, T2> Pair<T1, T2> parallel(Job<T1> job1, Job<T2> job2) throws Exception;

    <T> List<T> parallel(Job... jobs) throws Exception;

    Pair<Boolean, List<Object>> parallelGet(Job... jobs) throws Exception;

    Job<T> decorate(Job<T> job);

    Job<T> decorate(Job<T> job, ExecutionContext jobContext);

    /*
        Enqueue Methods:
     */
    <T> T enqueue(Job<T> job) throws Exception;

    <T> T enqueue(Class<Job> jobClass) throws Exception;
}
