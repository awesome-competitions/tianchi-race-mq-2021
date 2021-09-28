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

    public PMem(AnyMemoryBlock block, int capacity) {
        super(capacity);
        this.block = block;
    }


    @Override
    public ByteBuffer get() {
        byte[] bytes = new byte[capacity];
        block.copyToArray(0, bytes, 0, bytes.length);
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public void set(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.capacity()];
        buffer.get(bytes);
        block.copyFromArray(bytes, 0, 0, bytes.length);
        this.capacity = bytes.length;
    }

    @Override
    public void clear() {
        if (block.isValid()){
            block.freeMemory();
        }
    }

}
