package io.openmessaging.model;

import io.openmessaging.utils.CollectionUtils;
import sun.java2d.pipe.AAShapePipe;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Queue {

    private final int id;

    private Segment last;

    private final List<Segment> segments;

    private int offset;

    private final ReentrantLock lock;

    private byte[] data;

    public Queue(int id) {
        this.id = id;
        this.segments = new ArrayList<>();
        this.lock = new ReentrantLock();
    }

    public int getAndIncrementOffset(){
        return offset++;
    }

    public void lock(){
        lock.lock();
    }

    public void unlock(){
        lock.unlock();
    }

    public byte[] getData(long offset) {
        if (this.offset == offset){
            return data;
        }
        return null;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public void addSegment(Segment seg){
        seg.setIdx(this.segments.size());
        this.segments.add(seg);
        this.last = seg;
    }

    public int getId(){
        return id;
    }

    public Segment getLast() {
        return last;
    }

    public Segment getSegment(long offset){
        if (segments.isEmpty()){
            return null;
        }
        int left = 0;
        int right = segments.size() - 1;
        while(true){
            int index = (left + right) / 2;
            Segment mid = segments.get(index);
            if (offset < mid.getBeg()){
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

    public Segment nextSegment(Segment curr){
        if (curr.getIdx() < segments.size() - 1){
            return segments.get(curr.getIdx() + 1);
        }
        return null;
    }

}
