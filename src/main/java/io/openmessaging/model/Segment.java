package io.openmessaging.model;

import io.openmessaging.utils.BufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class Segment {

    private int beg;

    private int end;

    private long pos;

    private long aos;

    private int cap;

    private int idx;

    public Segment(int beg, int end, long pos, int cap) {
        this.beg = beg;
        this.end = end;
        this.pos = pos;
        this.aos = pos;
        this.cap = cap;
    }

    public boolean writable(int len){
        return cap - (aos - pos) >= len;
    }

    public void write(FileWrapper fw, ByteBuffer data) throws IOException {
        fw.write(aos, data);
        aos += data.capacity();
    }

    public int getBeg() {
        return beg;
    }

    public void setBeg(int beg) {
        this.beg = beg;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
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

    public int getCap() {
        return cap;
    }

    public void setCap(int cap) {
        this.cap = cap;
    }

    public int getIdx() {
        return idx;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }

    public List<ByteBuffer> getData(FileWrapper fw) {
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
}
