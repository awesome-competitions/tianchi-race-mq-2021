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

    private final Map<Long, Data> records;

    public Queue() {
        this.offset = -1;
        this.records = new HashMap<>();
    }

    public long write(FileWrapper fw, long position, ByteBuffer buffer){
        ++ offset;
        if (offset > 0){
            records.put(offset - 1, new SSD(fw, active.getPosition(), active.getCapacity()));
        }
        active.set(buffer);
        active.setPosition(position);
        return offset;
    }

    public List<ByteBuffer> read(long offset, int num){
        if (offset == this.offset){
            return Collections.singletonList(active.get());
        }
        List<ByteBuffer> buffers = new ArrayList<>();
        for (long i = offset; i < offset + num; i ++){
            if (i == this.offset){
                buffers.add(active.get());
                break;
            }
            Data data = records.get(offset);
            if (data != null){
                buffers.add(data.get());
            }
        }
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
