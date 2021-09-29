package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Block {

    private final AnyMemoryBlock block;

    private final long capacity;

    private final AtomicLong memPos;

    private final Map<Long, Long> offsets;

    private long aofPos;

    public Block(AnyMemoryBlock block, long capacity) {
        this.block = block;
        this.capacity = capacity;
        this.memPos = new AtomicLong();
        this.offsets = new ConcurrentHashMap<>();
    }

    public long allocate(int cap){
        long newPos = memPos.addAndGet(cap);
        if (newPos > this.capacity){
            memPos.addAndGet(-cap);
            return -1;
        }
        return newPos - cap;
    }

    public byte[] read(long position, int length){
        byte[] bytes = new byte[length];
        block.copyToArray(position, bytes, 0, length);
        return bytes;
    }

    public void write(long position, byte[] bytes){
        block.copyFromArray(bytes, 0, position, bytes.length);
    }

    public void register(long tid, long qid, long offset, long aofPos, long memPos){
        if (memPos == 0){
            this.aofPos = aofPos;
        }
        offsets.put(tid * 10000 + qid, offset);
    }

    public void unregister(long tid, long qid, long offset){
        long max = offsets.get(tid * 10000 + qid);
        if (offset >= max){
            offsets.remove(tid * 10000 + qid);
        }
    }
}
