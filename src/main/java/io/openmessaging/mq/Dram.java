package io.openmessaging.mq;


import java.nio.ByteBuffer;

public class Dram extends Data {

    private ByteBuffer data;

    private int size;

    public Dram(ByteBuffer data) {
        super(data.capacity());
        this.data = data;
        this.isDram = true;
    }

    @Override
    public ByteBuffer get() {
        return get(Threads.get());
    }

    @Override
    public ByteBuffer get(Threads.Context ctx) {
        ByteBuffer buffer = ctx.allocateBuffer();
        buffer.limit(size);
        buffer.put(data);
        buffer.flip();
        return buffer;
    }

    @Override
    public void set(ByteBuffer buffer) {
        this.data.put(buffer);
        this.data.flip();
        this.size = data.limit();
    }

    @Override
    public void clear() {
        data.clear();
    }
}
