package io.openmessaging.impl;

public class Config {

    private final String aofDir;
    private final long aofSize;
    private final String aepDir;
    private final long aepSize;
    private final int batch;
    private final int maxTopicSize;
    private final int maxQueueSize;
    private final long directSize;
    private final long heapSize;
    private final long liveTime;

    public Config(String aofDir, long aofSize, String aepDir, long aepSize, int batch, int maxTopicSize, int maxQueueSize, long directSize, long heapSize, long liveTime) {
        this.aofDir = aofDir;
        this.aofSize = aofSize;
        this.aepDir = aepDir;
        this.aepSize = aepSize;
        this.batch = batch;
        this.maxTopicSize = maxTopicSize;
        this.maxQueueSize = maxQueueSize;
        this.directSize = directSize;
        this.heapSize = heapSize;
        this.liveTime = liveTime;
    }

    public String getAofDir() {
        return aofDir;
    }

    public long getAofSize() {
        return aofSize;
    }

    public long getDirectSize() {
        return directSize;
    }

    public long getHeapSize() {
        return heapSize;
    }

    public String getAepDir() {
        return aepDir;
    }

    public long getAepSize() {
        return aepSize;
    }

    public long getLiveTime() {
        return liveTime;
    }

    public int getBatch() {
        return batch;
    }

    public int getMaxTopicSize() {
        return maxTopicSize;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }
}
