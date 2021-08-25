package io.openmessaging.model;

import io.openmessaging.utils.CollectionUtils;
import sun.java2d.pipe.AAShapePipe;

import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Queue {

    private int id;

    private final List<Allocate> allocates = new ArrayList<>();

    private final AtomicLong offset = new AtomicLong();

    private final ReentrantLock lock = new ReentrantLock();

    private MappedByteBuffer mappedByteBuffer;

    public Queue(int id){
        this.id = id;
    }

    public long nextOffset(){
        return offset.getAndIncrement();
    }

    public void mappedByteBuffer(MappedByteBuffer mappedByteBuffer){
        this.mappedByteBuffer = mappedByteBuffer;
    }

    public MappedByteBuffer mappedByteBuffer(){
        return mappedByteBuffer;
    }

    public synchronized void allocate(Allocate allocate){
        allocate.setIndex(allocates.size());
        if (! allocates.isEmpty()){
            lastOfAllocates().setEnd(allocate.getStart() - 1);
        }
        allocates.add(allocate);
    }

    public Allocate lastOfAllocates(){
        return CollectionUtils.lastOf(allocates);
    }

    public void lock(){
        lock.lock();
    }

    public void unlock(){
        lock.unlock();
    }

    public int id() {
        return id;
    }

    public Allocate search(long offset){
        if (allocates.isEmpty()){
            return null;
        }
        int left = 0;
        int right = allocates.size() - 1;
        while(true){
            int index = (left + right) / 2;
            Allocate mid = allocates.get(index);
            if (offset < mid.getStart()){
                right = index - 1;
                if (right < left) break;
            }else if (offset > mid.getEnd()){
                left = index + 1;
                if (left > right) break;
            }else{
                return mid;
            }
        }
        return null;
    }

    public Allocate next(Allocate allocate){
        if (allocate.getIndex() > allocates.size() - 2){
            return null;
        }
        return allocates.get(allocate.getIndex() + 1);
    }

}
