package io.openmessaging.mq;

import io.openmessaging.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Dram extends Data {

    private ByteBuffer data;

    public Dram(int capacity) {
        super(capacity);
    }


    @Override
    public ByteBuffer get() {
        return data;
    }

    @Override
    public void set(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.capacity()];
        buffer.get(bytes);
        this.data = ByteBuffer.wrap(bytes);
        this.capacity = bytes.length;
    }

    @Override
    public void clear() {
        this.data = null;
    }
}
