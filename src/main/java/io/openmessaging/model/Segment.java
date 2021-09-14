package io.openmessaging.model;

import io.openmessaging.cache.Storage;
import io.openmessaging.utils.BufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Segment {

    private long start;

    private long end;

    private long pos;

    private long aos;

    private long cap;

    private int idx;

    private ReadWriteLock lock;

    private Storage storage;

    public Segment(int start, int end, long pos, long cap) {
        this.start = start;
        this.end = end;
        this.pos = pos;
        this.aos = pos;
        this.cap = cap;
        this.lock = new ReentrantReadWriteLock();
    }

    public boolean writable(int len){
        return cap - (aos - pos) >= len;
    }

    public void write(FileWrapper fw, ByteBuffer data) throws IOException {
        fw.write(aos, data);
        aos += data.capacity();
    }

    public Storage getStorage() {
        return storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    public long getStart() {
        return start;
    }

    public void setBeg(long beg) {
        this.start = beg;
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

    public int getIdx() {
        return idx;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }

    public Lock readLock(){
        return lock.readLock();
    }

    public Lock writeLock(){
        return lock.writeLock();
    }

    public List<ByteBuffer> load(FileWrapper fw) {
        MappedByteBuffer mmb = null;
        List<ByteBuffer> data = null;
        try {
            mmb = fw.getChannel().map(FileChannel.MapMode.READ_ONLY, pos, aos - pos);
            short size;
            data = new ArrayList<>();
            while (mmb.remaining() > 2 && (size = mmb.getShort()) > 0){
                byte[] bytes = new byte[size];
                mmb.get(bytes);
                data.add(ByteBuffer.wrap(bytes));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (mmb != null){
                BufferUtils.clean(mmb);
            }
        }
        return data;
    }

    public byte[] loadBytes(FileWrapper fw) {
        if (aos == pos){
            return null;
        }
        MappedByteBuffer mmb = null;
        byte[] bytes = null;
        try {
            mmb = fw.getChannel().map(FileChannel.MapMode.READ_ONLY, pos, aos - pos);
            bytes = new byte[mmb.capacity()];
            mmb.get(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (mmb != null){
                BufferUtils.clean(mmb);
            }
        }
        return bytes;
    }

    public void clean(){
        Storage s = storage;
        if (s != null){
            this.storage = null;
            s.clean();
        }
    }
}
