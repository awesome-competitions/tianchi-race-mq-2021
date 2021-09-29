package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Block {

    private final AnyMemoryBlock block;

    private final long capacity;

    private final AtomicLong position;

    public Block(AnyMemoryBlock block, long capacity) {
        this.block = block;
        this.capacity = capacity;
        this.position = new AtomicLong();
    }

    public long allocate(int capacity){
        long pos = position.addAndGet(capacity);
        if (pos > this.capacity){
            return -1;
        }
        return pos - capacity;
    }

    public byte[] read(long position, int length){
        byte[] bytes = new byte[length];
        block.copyToArray(position, bytes, 0, length);
        return bytes;
    }

    public AnyMemoryBlock getBlock() {
        return block;
    }
}
