package io.openmessaging.mq;

import io.openmessaging.consts.Const;

public class Config {

    private final String dataDir;
    private final String heapDir;
    private final long heapSize;
    private final int maxCount;
    private final long cacheMaxSize;
    private final long cacheClearSize;

    public Config(String dataDir, String heapDir, long heapSize, int maxCount, long cacheMaxSize, long cacheClearSize) {
        this.dataDir = dataDir;
        this.heapDir = heapDir;
        this.heapSize = heapSize;
        this.maxCount = maxCount;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheClearSize = cacheClearSize;
    }

    public long getCacheMaxSize() {
        return cacheMaxSize;
    }

    public long getCacheClearSize() {
        return cacheClearSize;
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

    public int getMaxCount() {
        return maxCount;
    }
}
