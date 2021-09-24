package io.openmessaging.model;

import io.openmessaging.cache.Storage;
import io.openmessaging.utils.BufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Segment {

    private Queue queue;

    private int tid;

    private int qid;

    private long start;

    private long end;

    private long pos;

    private long aos;

    private long cap;

    private int idx;

    private Storage storage;

    private ReentrantLock lock;

    public Segment(Queue queue, int tid, int qid, long start, long end, long cap) {
        this.queue = queue;
        this.tid = tid;
        this.qid = qid;
        this.start = start;
        this.end = end;
        this.cap = cap;
        this.lock = new ReentrantLock();
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public long getAos() {
        return aos;
    }

    public void setAos(long aos) {
        this.aos = aos;
    }

    public long getCap() {
        return cap;
    }

    public void setCap(long cap) {
        this.cap = cap;
    }

    public void lock() {
        lock.lock();
    }

    public void unlock(){
        lock.unlock();
    }

    public int getIdx() {
        return idx;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }

    public int getQid() {
        return qid;
    }

    public Storage getStorage() {
        return storage;
    }

    public Queue getQueue() {
        return queue;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    public boolean writable(int len){
        return cap - pos >= len;
    }

    public void write(ByteBuffer byteBuffer){
//        storage.write(byteBuffer);
        pos += byteBuffer.capacity();
    }

    public List<ByteBuffer> read(long startOffset, long endOffset){
        return storage.read(startOffset, endOffset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Segment segment = (Segment) o;
        return tid == segment.tid && qid == segment.qid && idx == segment.idx;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tid, qid, idx);
    }
}
