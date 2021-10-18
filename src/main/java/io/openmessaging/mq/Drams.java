package io.openmessaging.mq;


import io.openmessaging.utils.BufferUtils;

import java.nio.ByteBuffer;

public class Drams extends Data {

    private ByteBuffer data1;

    private ByteBuffer data2;

    private ByteBuffer data3;

    private int size;

    private ByteBuffer ext;

    public Drams(ByteBuffer data1, ByteBuffer data2, ByteBuffer data3) {
        super(data1.capacity() + data2.capacity() + data3.capacity());
        this.data1 = data1;
        this.data2 = data2;
        this.data3 = data3;
        this.isDram = true;
    }

    @Override
    public ByteBuffer get() {
        return get(Threads.get());
    }

    @Override
    public ByteBuffer get(Threads.Context ctx) {
        return null;
    }

    @Override
    public void set(ByteBuffer buffer) {
        return;
    }

    @Override
    public void clear() {
        if (this.ext != null){
            Monitor.extSize -= ext.capacity();
            BufferUtils.clean(ext);
        }
        ext = null;
    }
}
