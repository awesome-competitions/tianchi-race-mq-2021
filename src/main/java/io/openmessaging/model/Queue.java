package io.openmessaging.model;

import io.openmessaging.cache.Cache;
import io.openmessaging.cache.PMem;
import io.openmessaging.cache.Storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Queue {

    private final int id;

    private Segment head;

    private Segment last;

    private final List<Segment> segments;

    private long offset;

    private byte[] data;

    private ReadWriteLock lock;

    private Storage storage;

    private long readOffset;

    public Queue(int id) {
        this.id = id;
        this.segments = new ArrayList<>();
        this.lock = new ReentrantReadWriteLock();
    }

    public long getAndIncrementOffset(){
        return offset++;
    }

    public long getOffset() {
        return offset;
    }

    public byte[] getData(long offset) {
        if (this.offset == offset){
            return data;
        }
        return null;
    }

    public ReadWriteLock getLock() {
        return lock;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public void addSegment(Segment seg){
        seg.setIdx(this.segments.size());
        if (this.head != null){
            this.head.setEnd(seg.getStart() - 1);
        }
        this.segments.add(seg);
        this.head = seg;
    }

    public int getId(){
        return id;
    }

    public Storage getStorage() {
        return storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    public Segment getHead() {
        return head;
    }

    public Segment getLast(long offset) {
        if (last == null){
            last = getSegment(offset);
        }
        return last;
    }

    public Segment getLast(){
        return last;
    }

    public long getReadOffset() {
        return readOffset;
    }

    public void setReadOffset(long readOffset) {
        this.readOffset = readOffset;
    }

    public List<Segment> getSegments() {
        return segments;
    }

    public void setLast(Cache cache, Segment newLast) {
        if (last != null && last.getIdx() != newLast.getIdx()){
            cache.clearSegment(last);
        }
        this.last = newLast;
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

    public Segment nextSegment(Segment curr){
        if (curr.getIdx() < segments.size() - 1){
            return segments.get(curr.getIdx() + 1);
        }
        return null;
    }
}
