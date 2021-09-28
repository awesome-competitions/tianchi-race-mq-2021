package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import io.openmessaging.utils.CollectionUtils;

import java.awt.font.FontRenderContext;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class Queue {

    private long offset;

    private Data active;

    private long readOffset;

    private final Map<Long, Data> stables;

    public Queue() {
        this.offset = -1;
        this.stables = new HashMap<>();
    }

    public long write(FileWrapper fw, ByteBuffer buffer){
        ++ offset;
        if (! active.writable(buffer.capacity())){
//            try {
//                if (active.getEnd() >= readOffset){
//                    ByteBuffer data = active.load();
//                    Monitor.writeDistCount ++;
//                    long position = fw.write(data);
//                    Data stable = new SSD(active.getStart(), active.getEnd(), position, data.capacity(), fw, active.getRecords());
//                    for (long i = stable.getStart(); i <= active.getEnd(); i ++){
//                        stables.put(i, stable);
//                    }
//                }
//                this.active.reset(offset, offset, 0, null, null);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
            this.active.reset(offset, offset, 0, null, null);
        }
        active.write(buffer);
        return offset;
    }

    public List<ByteBuffer> read(long offset, int num){
        if (offset > this.offset){
            return null;
        }
        int fetchedNum = num;
        long startOffset = offset;
        List<ByteBuffer> buffers = new ArrayList<>(num);
        while (fetchedNum > 0 && startOffset <= this.offset){
            List<ByteBuffer> data = null;
            if (startOffset >= active.getStart()){
                data = active.read(startOffset, fetchedNum);
            }else{
                Data reader = stables.get(startOffset);
                if (reader != null){
                    Monitor.readDistCount ++;
                    data = reader.read(startOffset, fetchedNum);
                }
            }
            if (data == null){
                break;
            }
            buffers.addAll(data);
            fetchedNum -= data.size();
            startOffset += data.size();
        }
        readOffset = offset + buffers.size();
        return buffers;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setActive(Data active) {
        this.active = active;
    }

}
