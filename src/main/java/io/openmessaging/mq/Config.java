package io.openmessaging.mq;

import io.openmessaging.consts.Const;

public class Config {

    private String dataDir;
    private String heapDir;
    private long heapSize;
    private int maxCount;
    private long cacheMaxSize;
    private long cacheClearSize;

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
