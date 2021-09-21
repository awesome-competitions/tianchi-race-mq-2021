package io.openmessaging.model;

public class Config {

    private final String dataDir;
    private final String heapDir;
    private final long heapSize;
    private final long pageSize;
    private final int groupSize;
    private final int lruSize;
    private final int batchSize;

    public Config(String dataDir, String heapDir, long heapSize, int lruSize, long pageSize, int groupSize, int batchSize) {
        this.dataDir = dataDir;
        this.heapDir = heapDir;
        this.heapSize = heapSize;
        this.pageSize = pageSize;
        this.groupSize = groupSize;
        this.lruSize = lruSize;
        this.batchSize = batchSize;
    }

    public Config(String dataDir, String heapDir, long heapSize, long pageSize, int groupSize) {
        this(dataDir, heapDir, heapSize, 1000, pageSize, groupSize, 30);
    }

    public Config(String dataDir, long pageSize, int groupSize) {
        this(dataDir, null, 0, pageSize, groupSize);
    }

    public Config(String dataDir, int lruSize, long pageSize, int groupSize) {
        this(dataDir, null, 0, lruSize, pageSize, groupSize, 30);
    }

    public long getPageSize() {
        return pageSize;
    }

    public int getGroupSize() {
        return groupSize;
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

    public int getLruSize() {
        return lruSize;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
