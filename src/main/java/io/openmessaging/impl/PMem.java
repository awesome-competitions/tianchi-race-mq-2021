package io.openmessaging.impl;

import java.nio.ByteBuffer;

public class PMem extends Data {

    private final Aep aep;

    private ByteBuffer ext;

    private int size;

    public PMem(Aep aep, long position, int capacity) {
        super(capacity);
        this.position = position;
        this.aep = aep;
        this.size = capacity;
        this.isPMem = true;
    }

    @Override
    public ByteBuffer get() {
        return get(Threads.get());
    }

    @Override
    public ByteBuffer get(Threads.Context ctx) {
        int extSize = ext == null ? 0 : ext.limit();

        ByteBuffer buffer = ctx.allocateBuffer();
        buffer.limit(size - extSize);
        aep.read(position, buffer);
        if (extSize > 0){
            buffer.limit(size);
            buffer.put(ext);
        }
        buffer.flip();
        return buffer;
    }

    @Override
    public void set(ByteBuffer buffer) {
        this.size = buffer.limit();

        buffer.limit(Math.min(size, capacity));
        aep.write(position, buffer);

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
        this.ext = null;
    }

}
