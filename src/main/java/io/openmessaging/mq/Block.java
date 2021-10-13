package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Block {

    private final FileWrapper fw;

    private final long capacity;

    private final AtomicLong memPos;

    private final Map<Long, Long> offsets;

    public Block(FileWrapper fw, long capacity) {
        this.fw = fw;
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

    public void read(long position, ByteBuffer buffer){
        try {
            fw.read(position, buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(long position, ByteBuffer buffer){
        try {
            fw.write(position, buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public FileWrapper getFw() {
        return fw;
    }
}
