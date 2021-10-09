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

    public Dram(ByteBuffer data) {
        super(data.capacity());
        this.data = data;
    }

    @Override
    public ByteBuffer get() {
        return data;
    }

    @Override
    public void set(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.limit()];
        buffer.get(bytes);
        this.data = ByteBuffer.wrap(bytes);
        this.capacity = bytes.length;
        Monitor.writeDramCount ++;
    }

    @Override
    public void clear() {
        this.data = null;
    }
}
