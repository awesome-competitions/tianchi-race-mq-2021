package io.openmessaging.mq;

import io.openmessaging.consts.Const;

public class Config {

    private final String dataDir;
    private final String heapDir;
    private final long heapSize;
    private final long heapUsableSize;
    private final int maxCount;
    private final int pageSize;
    private final long liveTime;

    public Config(String dataDir, String heapDir, long heapSize, long heapUsableSize, int maxCount, int pageSize, long liveTime) {
        this.dataDir = dataDir;
        this.heapDir = heapDir;
        this.heapSize = heapSize;
        this.heapUsableSize = heapUsableSize;
        this.maxCount = maxCount;
        this.pageSize = pageSize;
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

    public long getHeapUsableSize() {
        return heapUsableSize;
    }

    public int getMaxCount() {
        return maxCount;
    }

    public long getLiveTime() {
        return liveTime;
    }

    public int getPageSize() {
        return pageSize;
    }
}
