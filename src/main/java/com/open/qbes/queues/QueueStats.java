package com.open.qbes.queues;

public class QueueStats {

    private long jobsProcessed;
    private int runningJobs;
    private double averageProcessingTime;
    private double throughput;
    private long capacity;
    private long size;
    private boolean isShutDown;
    private boolean isShuttingDown;
    private int corePoolSize;
    private int maxPoolSize;
    private long keepAliveTime;
    private String mode;
    private String type;
    private String cacheType;
    private int largestPoolSize;
    private String queueId;

    public String getQueueId() {
        return queueId;
    }

    public void setQueueId(String queueId) {
        this.queueId = queueId;
    }

    public int getLargestPoolSize() {
        return largestPoolSize;
    }

    public void setLargestPoolSize(int largestPoolSize) {
        this.largestPoolSize = largestPoolSize;
    }

    public long getJobsProcessed() {
        return jobsProcessed;
    }

    public void setJobsProcessed(long jobsProcessed) {
        this.jobsProcessed = jobsProcessed;
    }

    public int getRunningJobs() {
        return runningJobs;
    }

    public void setRunningJobs(int runningJobs) {
        this.runningJobs = runningJobs;
    }

    public double getAverageProcessingTime() {
        return averageProcessingTime;
    }

    public void setAverageProcessingTime(double averageProcessingTime) {
        this.averageProcessingTime = averageProcessingTime;
    }

    public double getThroughput() {
        return throughput;
    }

    public void setThroughput(double throughput) {
        this.throughput = throughput;
    }

    public long getCapacity() {
        return capacity;
    }

    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public boolean isShutDown() {
        return isShutDown;
    }

    public void setShutDown(boolean shutDown) {
        isShutDown = shutDown;
    }

    public boolean isShuttingDown() {
        return isShuttingDown;
    }

    public void setShuttingDown(boolean shuttingDown) {
        isShuttingDown = shuttingDown;
    }

    public int getCorePoolSize() {
        return corePoolSize;
    }

    public void setCorePoolSize(int corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public long getKeepAliveTime() {
        return keepAliveTime;
    }

    public void setKeepAliveTime(long keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCacheType() {
        return cacheType;
    }

    public void setCacheType(String cacheType) {
        this.cacheType = cacheType;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("QueueStats");
        sb.append("{jobsProcessed=").append(jobsProcessed);
        sb.append(", runningJobs=").append(runningJobs);
        sb.append(", averageProcessingTime=").append(averageProcessingTime);
        sb.append(", throughput=").append(throughput);
        sb.append(", capacity=").append(capacity);
        sb.append(", size=").append(size);
        sb.append(", isShutDown=").append(isShutDown);
        sb.append(", isShuttingDown=").append(isShuttingDown);
        sb.append(", corePoolSize=").append(corePoolSize);
        sb.append(", maxPoolSize=").append(maxPoolSize);
        sb.append(", keepAliveTime=").append(keepAliveTime);
        sb.append(", mode='").append(mode).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", cacheType='").append(cacheType).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
