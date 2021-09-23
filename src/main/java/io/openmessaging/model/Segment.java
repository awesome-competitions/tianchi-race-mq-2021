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
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Segment {

    private int qid;

    private long start;

    private long end;

    private long pos;

    private long aos;

    private long cap;

    private int idx;

    private long vos;

    public Segment(long start, long end, long pos, long cap) {
        this.start = start;
        this.end = end;
        this.pos = pos;
        this.aos = pos;
        this.vos = vos;
        this.cap = cap;
    }

    public Segment(int qid, long start, long end, long pos, long cap) {
        this.qid = qid;
        this.start = start;
        this.end = end;
        this.pos = pos;
        this.aos = pos;
        this.vos = vos;
        this.cap = cap;
    }

    public boolean writable(int len){
        return cap - (vos - pos) >= len;
    }

    public void preWrite(ByteBuffer buffer){
        vos += buffer.capacity();
    }

    public void write(FileWrapper fw, ByteBuffer buffer) throws IOException {
        fw.write(aos, buffer);
        aos += buffer.capacity();
        vos = aos;
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

    public int getIdx() {
        return idx;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }

    public int getQid() {
        return qid;
    }

    public List<ByteBuffer> load(FileWrapper fw, boolean direct) {
        List<ByteBuffer> data = null;
        try {

            long allocateSize = cap;
            if (aos > pos){
                allocateSize = aos - pos;
            }
            ByteBuffer byteBuffer = ByteBuffer.allocate((int) allocateSize);
            fw.read(pos, byteBuffer);
            byteBuffer.flip();

            short size;
            data = new ArrayList<>();
            while (byteBuffer.remaining() > 2 && (size = byteBuffer.getShort()) > 0){
                byte[] bytes = new byte[size];
                byteBuffer.get(bytes);
                if (direct){
                    ByteBuffer directBuffer = ByteBuffer.allocateDirect(bytes.length);
                    directBuffer.put(bytes);
                    directBuffer.flip();
                    data.add(directBuffer);
                    continue;
                }
                data.add(ByteBuffer.wrap(bytes));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    public void reset(FileWrapper fw) {
        MappedByteBuffer mmb = null;
        try {
            mmb = fw.getChannel().map(FileChannel.MapMode.READ_ONLY, pos, cap);
            short size;
            int count = 0;
            while (mmb.remaining() > 2 && (size = mmb.getShort()) > 0){
                mmb.position(mmb.position() + size);
                count ++;
            }
            if (mmb.position() > 2){
                this.aos = this.pos + mmb.position() - 2;
                this.end = this.start + count - 1;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (mmb != null){
                BufferUtils.clean(mmb);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Segment segment = (Segment) o;
        return pos == segment.pos;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pos);
    }
}
