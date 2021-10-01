package io.openmessaging.mq;

public class Record {

    private final long offset;

    private final long capacity;

    public Record(long offset, long capacity) {
        this.offset = offset;
        this.capacity = capacity;
    }

    public long getOffset() {
        return offset;
    }

    public long getCapacity() {
        return capacity;
    }
}
