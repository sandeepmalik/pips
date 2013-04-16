package com.open.qbes.core;

import com.open.qbes.core.annotations.ContextConfiguration;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Sandeep Malik
 * Date: 12/7/12
 * Time: 4:28 PM
 */
public class ExecutionResult {

    private final ContextConfiguration.Strategy strategy;
    private final Map<Job, Object> finishedJobs;
    private final ContextConfiguration.ExecutionState executionState;

    public ContextConfiguration.Strategy getStrategy() {
        return strategy;
    }

    public Map<Job, Object> getFinishedJobs() {
        return finishedJobs;
    }

    public ExecutionResult(ContextConfiguration.Strategy strategy, Map<Job, Object> finishedJobs, ContextConfiguration.ExecutionState executionState) {
        this.strategy = strategy;
        this.finishedJobs = finishedJobs;
        this.executionState = executionState;
    }
}
