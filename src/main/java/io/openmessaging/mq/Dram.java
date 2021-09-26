package io.openmessaging.mq;

import java.nio.ByteBuffer;

public class Dram extends Storage{

    private ByteBuffer buffer;

    public Dram(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public ByteBuffer get() {
        return ByteBuffer.wrap(buffer.array());
    }

    @Override
    public void set(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public void clear() {
        this.buffer.clear();
    }

    @Override
    public int size() {
        return buffer.capacity();
    }
}
