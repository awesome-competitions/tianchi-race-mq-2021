package io.openmessaging.model;

import com.intel.pmem.llpl.AnyMemoryBlock;
import io.openmessaging.cache.AbstractMedium;
import io.openmessaging.utils.BufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class Segment {

    private long beg;

    private long end;

    private long pos;

    private long aos;

    private int cap;

    private int idx;

    private AbstractMedium cache;

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
//        fw.write(aos, data);
        aos += data.capacity();
    }

    public long getBeg() {
        return beg;
    }

    public void setBeg(long beg) {
        this.beg = beg;
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

    public AbstractMedium getCache() {
        return cache;
    }

    public void setCache(AbstractMedium cache) {
        this.cache = cache;
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
}
