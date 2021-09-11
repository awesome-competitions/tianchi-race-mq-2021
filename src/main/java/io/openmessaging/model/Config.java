package io.openmessaging.model;

public class Config {

    private final String dataDir;
    private final String heapDir;
    private final long heapSize;
    private final int pageSize;
    private final int cacheSize;
    private final int groupSize;
    private final int queueSize;

    public Config(String dataDir, String heapDir, long heapSize, int pageSize, int cacheSize, int groupSize, int queueSize) {
        this.dataDir = dataDir;
        this.heapDir = heapDir;
        this.heapSize = heapSize;
        this.pageSize = pageSize;
        this.cacheSize = cacheSize;
        this.groupSize = groupSize;
        this.queueSize = queueSize;
    }

    public Config(String dataDir, int pageSize, int cacheSize, int groupSize, int queueSize) {
        this(dataDir, null, 0, pageSize, cacheSize, groupSize, queueSize);
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

    public String getHeapDir() {
        return heapDir;
    }

    public long getHeapSize() {
        return heapSize;
    }
}
