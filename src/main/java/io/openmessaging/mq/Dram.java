package io.openmessaging.mq;


import java.nio.ByteBuffer;

public class Dram extends Data {

    private ByteBuffer data;

    public Dram(int capacity) {
        super(capacity);
    }

    public Dram(ByteBuffer data) {
        super(data.capacity());
        this.data = data;
    }

    @Override
    public ByteBuffer get() {
        ByteBuffer buffer = Buffers.allocateBuffer();
        buffer.limit(capacity);
        buffer.put(data);
        buffer.flip();
        return buffer;
    }

    @Override
    public void set(ByteBuffer buffer) {
        this.data.put(buffer);
        this.data.flip();
        this.capacity = data.limit();
    }

    @Override
    public void clear() {
        data.clear();
    }
}
