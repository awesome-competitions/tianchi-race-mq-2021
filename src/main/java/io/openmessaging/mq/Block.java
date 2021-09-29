package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Block {

    private final int id;

    private final AnyMemoryBlock block;

    private final long capacity;

    private final AtomicLong memPos;

    private final Map<Long, Long> offsets;

    public Block(int id, AnyMemoryBlock block, long capacity) {
        this.id = id;
        this.block = block;
        this.capacity = capacity;
        this.memPos = new AtomicLong();
        this.offsets = new ConcurrentHashMap<>();
    }

    public int getId() {
        return id;
    }

    public void reset(){
        this.memPos.set(0);
        this.offsets.clear();
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

    public void register(long tid, long qid, long offset){
        offsets.put(tid * 10000 + qid, offset);
    }

    public void unregister(long tid, long qid, long offset){
        Long max = offsets.get(tid * 10000 + qid);
        if (max != null && offset >= max){
            offsets.remove(tid * 10000 + qid);
        }
    }

    public boolean isFree(){
        return offsets.isEmpty();
    }
}
