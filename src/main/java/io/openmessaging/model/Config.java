package io.openmessaging.model;

public class Config {

    private final String dataDir;
    private final String heapDir;
    private final long heapSize;
    private final long pageSize;
    private final int groupSize;

    public Config(String dataDir, String heapDir, long heapSize, long pageSize, int groupSize) {
        this.dataDir = dataDir;
        this.heapDir = heapDir;
        this.heapSize = heapSize;
        this.pageSize = pageSize;
        this.groupSize = groupSize;
    }

    public Config(String dataDir, long pageSize, int groupSize) {
        this(dataDir, null, 0, pageSize, groupSize);
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
}
