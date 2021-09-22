package io.openmessaging.model;

import io.openmessaging.consts.Const;

public class Config {

    private final String dataDir;
    private final String heapDir;
    private final long heapSize;
    private final long pageSize;
    private final int groupSize;
    private final int lruSize;
    private final int maxCount;
    private final long maxSize;

    public Config(String dataDir, String heapDir, long heapSize, int lruSize, long pageSize, int groupSize, int maxCount, long maxSize) {
        this.dataDir = dataDir;
        this.heapDir = heapDir;
        this.heapSize = heapSize;
        this.pageSize = pageSize;
        this.groupSize = groupSize;
        this.lruSize = lruSize;
        this.maxCount = maxCount;
        this.maxSize = maxSize;
    }

    public Config(String dataDir, String heapDir, long heapSize, long pageSize, int groupSize) {
        this(dataDir, heapDir, heapSize, 1000, pageSize, groupSize, 30, Const.K * 64);
    }

    public Config(String dataDir, long pageSize, int groupSize) {
        this(dataDir, null, 0, pageSize, groupSize);
    }

    public Config(String dataDir, int lruSize, long pageSize, int groupSize) {
        this(dataDir, null, 0, lruSize, pageSize, groupSize, 30, Const.K * 64);
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

    public int getMaxCount() {
        return maxCount;
    }

    public long getMaxSize() {
        return maxSize;
    }
}
