package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;

import java.nio.ByteBuffer;
import java.util.concurrent.*;

public class PMem extends Data {

    private final Future<AnyMemoryBlock> future;

    private final int size;

    public PMem(Future<AnyMemoryBlock> future, byte[] bytes) {
        this.future = future;
        this.size = bytes.length;
    }

    @Override
    public ByteBuffer get() {
        try {
            byte[] bytes = new byte[size];
            AnyMemoryBlock block = future.get();
            block.copyToArray(0, bytes, 0, bytes.length);
            return ByteBuffer.wrap(bytes);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void set(ByteBuffer buffer) {
        throw new UnsupportedOperationException("p-mem not support set.");
    }

    @Override
    public void clear() {
        try {
            AnyMemoryBlock block = future.get();
            if (block.isValid()){
                block.freeMemory();
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long size() {
        return size;
    }
}
