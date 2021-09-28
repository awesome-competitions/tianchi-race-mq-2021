package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import io.openmessaging.utils.CollectionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Queue {

    private long offset;

    private Data active;

    private Data reader;

    private int readerIndex;

    private List<Data> stables;

    public Queue() {
        this.offset = -1;
        this.stables = new ArrayList<>();
    }

    public long write(FileWrapper fw, ByteBuffer buffer){
        ++ offset;
        if (! active.writable(buffer.capacity())){
            try {
                ByteBuffer data = active.load();
                long position = fw.write(data);
                Data stable = new SSD(active.getStart(), active.getEnd(), position, data.capacity(), fw, active.getRecords());
                this.stables.add(stable);
                this.active.reset(offset, offset, 0, null, null);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        active.write(buffer);
        return offset;
    }

    public List<ByteBuffer> read(long offset, int num){
        if (offset >= active.getStart()){
            return active.read(offset, num);
        }
        if (reader == null){
            reader = stables.get(0);
        }
        int fetchedNum = num;
        long startOffset = offset;
        List<ByteBuffer> buffers = new ArrayList<>(num);
        while(fetchedNum > 0){
            if (startOffset > reader.getEnd()){
                ++readerIndex;
                if (readerIndex == stables.size()){
                    reader = active;
                }else if (readerIndex > stables.size()){
                    break;
                }else{
                    reader = stables.get(readerIndex);
                }
            }
            List<ByteBuffer> data = reader.read(offset, num);
            if (CollectionUtils.isEmpty(data)){
                break;
            }
            fetchedNum -= data.size();
            startOffset += data.size();
            buffers.addAll(data);
        }
        return buffers;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public Data getActive() {
        return active;
    }

    public void setActive(Data active) {
        this.active = active;
    }

    public Data getReader() {
        return reader;
    }

    public void setReader(Data reader) {
        this.reader = reader;
    }

    public List<Data> getStables() {
        return stables;
    }

    public void setStables(List<Data> stables) {
        this.stables = stables;
    }
}
