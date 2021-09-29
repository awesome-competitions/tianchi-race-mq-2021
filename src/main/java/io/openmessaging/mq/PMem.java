package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
import io.openmessaging.utils.CollectionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

public class PMem extends Data {

    private final AnyMemoryBlock block;

    public PMem(AnyMemoryBlock block, long position, int capacity) {
        super(capacity);
        this.position = position;
        this.block = block;
    }

    @Override
    public ByteBuffer get() {
        Monitor.readMemCount ++;
        byte[] bytes = new byte[capacity];
        block.copyToArray(position, bytes, 0, bytes.length);
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public void set(ByteBuffer buffer) {
        Monitor.writeMemCount ++;
        byte[] bytes = new byte[buffer.limit()];
        buffer.get(bytes);
        block.copyFromArray(bytes, 0, position, bytes.length);
        this.capacity = bytes.length;
    }

    @Override
    public void clear() {}

}
