package io.openmessaging.mq;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class Data {

    protected int capacity;

    protected long position;

    public Data(int capacity) {
        this.capacity = capacity;
    }

    public abstract ByteBuffer get();

    public abstract void set(ByteBuffer buffer);

    public abstract void clear();

    public int getCapacity() {
        return capacity;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public String toString() {
        return "Data{" +
                "capacity=" + capacity +
                ", position=" + position +
                '}';
    }
}
