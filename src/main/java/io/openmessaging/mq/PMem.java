package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;

import java.nio.ByteBuffer;

public class PMem extends Data {

    private final AnyMemoryBlock block;

    private final int size;

    public PMem(AnyMemoryBlock block, int size) {
        this.block = block;
        this.size = size;
    }

    @Override
    public ByteBuffer get() {
        byte[] bytes = new byte[(int) block.size()];
        block.copyToArray(0, bytes, 0, bytes.length);
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public void set(ByteBuffer buffer) {
        throw new UnsupportedOperationException("p-mem not support set.");
    }

    @Override
    public void clear() {
        if (this.block.isValid()){
            this.block.freeMemory();
        }
    }

    @Override
    public long size() {
        return size;
    }
}
