package io.openmessaging.impl;

import java.nio.ByteBuffer;

public class Buffers {

    private final long directSize;
    private final long dramSize;
    private boolean completed;
    private long writeSize;

    public Buffers(long directSize, long heapSize) {
        this.directSize = directSize;
        this.dramSize = directSize + heapSize;
    }

    public Data allocateBuffer(int cap){
        if (completed){
            return null;
        }
        if (writeSize < directSize){
            writeSize += cap;
            return new Dram(ByteBuffer.allocateDirect(cap));
        }else if (writeSize < dramSize){
            writeSize += cap;
            return new Dram(ByteBuffer.allocate(cap));
        }
        completed = true;
        return null;
    }

    public boolean completed(){
        return completed;
    }
}
