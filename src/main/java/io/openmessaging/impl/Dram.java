package io.openmessaging.impl;


import java.nio.ByteBuffer;

public class Dram extends Data {

    private ByteBuffer data;

    private int size;

    private ByteBuffer ext;

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
        buffer.put(data);
        if (ext != null){
            buffer.put(ext);
        }
        buffer.flip();
        return buffer;
    }

    @Override
    public void set(ByteBuffer buffer) {
        this.size = buffer.limit();
        buffer.limit(Math.min(size, capacity));
        this.data.put(buffer);
        this.data.flip();
        if (size > capacity){
            buffer.limit(size);
            ext = ByteBuffer.allocateDirect(buffer.remaining());
            ext.put(buffer);
            ext.flip();
        }
    }

    @Override
    public void clear() {
        if (this.ext != null){
            Utils.recycleByteBuffer(ext);
        }
        ext = null;
        data.clear();
    }

    public ByteBuffer getData() {
        return data;
    }
}
