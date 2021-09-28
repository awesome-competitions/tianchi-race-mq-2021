package io.openmessaging.mq;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class Data {

    protected final int capacity;

    protected long start;

    protected long end;

    protected long position;

    protected List<Integer> records;

    public Data(int capacity) {
        this.capacity = capacity;
        this.records = new ArrayList<>();
    }

    public abstract boolean writable(int size);

    public abstract void write(ByteBuffer buffer);

    public abstract List<ByteBuffer> read(long offset, int num);

    public abstract ByteBuffer load();

    public abstract long size();

    public abstract void reset(long start, long end, long position, ByteBuffer buffer, List<Integer> records);

    public int getCapacity() {
        return capacity;
    }

    public long getStart() {
        return start;
    }

    public long getPosition() {
        return position;
    }

    public List<Integer> getRecords() {
        return records;
    }

    public long getEnd() {
        return end;
    }

    @Override
    public String toString() {
        return "Data{" +
                "capacity=" + capacity +
                ", start=" + start +
                ", end=" + end +
                ", position=" + position +
                ", records=" + records +
                '}';
    }
}
