package io.openmessaging.model;

public class Config {

    private final String dataDir;
    private final int pageSize;
    private final int cacheSize;
    private final int groupSize;
    private final int queueSize;

    public Config(String dataDir, int pageSize, int cacheSize, int groupSize, int queueSize) {
        this.dataDir = dataDir;
        this.pageSize = pageSize;
        this.cacheSize = cacheSize;
        this.groupSize = groupSize;
        this.queueSize = queueSize;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public int getGroupSize() {
        return groupSize;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public String getDataDir() {
        return dataDir;
    }
}
