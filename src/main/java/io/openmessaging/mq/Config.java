package io.openmessaging.mq;

import io.openmessaging.consts.Const;

public class Config {

    private String dataDir;
    private String heapDir;
    private long heapSize;
    private long topicMaxSize;
    private long topicShrinkSize;
    private int maxCount;

    public long getTopicMaxSize() {
        return topicMaxSize;
    }

    public long getTopicShrinkSize() {
        return topicShrinkSize;
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
