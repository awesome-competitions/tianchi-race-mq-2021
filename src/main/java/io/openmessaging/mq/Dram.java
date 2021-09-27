package io.openmessaging.mq;

import java.nio.ByteBuffer;

public class Dram extends Data {

    private ByteBuffer buffer;

    private final long size;

    public Dram(ByteBuffer buffer) {
        this.buffer = ByteBuffer.allocate(buffer.capacity());
        this.buffer.put(buffer);
        this.buffer.flip();
        this.size = buffer.capacity();
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
        this.buffer = null;
    }

    @Override
    public long size() {
        return size;
    }
}
