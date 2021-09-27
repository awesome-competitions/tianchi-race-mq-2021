package io.openmessaging.mq;

import io.openmessaging.consts.Const;

public class Config {

    private final String dataDir;
    private final String heapDir;
    private final long heapSize;
    private final int maxCount;
    private final int activeSize;
    private final int readerSize;
    private final long liveTime;

    public Config(String dataDir, String heapDir, long heapSize, int maxCount, int activeSize, int readerSize) {
        this(dataDir, heapDir, heapSize, maxCount, activeSize, readerSize, 0);
    }

    public Config(String dataDir, String heapDir, long heapSize, int maxCount, int activeSize, int readerSize, long liveTime) {
        this.dataDir = dataDir;
        this.heapDir = heapDir;
        this.heapSize = heapSize;
        this.maxCount = maxCount;
        this.activeSize = activeSize;
        this.readerSize = readerSize;
        this.liveTime = liveTime;
    }

    public long getLiveTime() {
        return liveTime;
    }

    public int getActiveSize() {
        return activeSize;
    }

    public int getReaderSize() {
        return readerSize;
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
