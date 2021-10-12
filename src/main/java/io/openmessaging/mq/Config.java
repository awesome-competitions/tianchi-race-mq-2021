package io.openmessaging.mq;

import io.openmessaging.consts.Const;

public class Config {

    private final String dataDir;
    private final String heapDir;
    private final long heapSize;
    private final int maxCount;
    private final int batch;
    private final long liveTime;

    public Config(String dataDir, String heapDir, long heapSize, int maxCount, int batch, long liveTime) {
        this.dataDir = dataDir;
        this.heapDir = heapDir;
        this.heapSize = heapSize;
        this.maxCount = maxCount;
        this.batch = batch;
        this.liveTime = liveTime;
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

    public long getLiveTime() {
        return liveTime;
    }

    public int getBatch() {
        return batch;
    }
}
