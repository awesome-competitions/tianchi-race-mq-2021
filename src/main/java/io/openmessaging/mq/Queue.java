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

    private final Cache cache;

    private final FileWrapper fw;

    public Queue(Cache cache, FileWrapper fw) {
        this.cache = cache;
        this.fw = fw;
        this.offset = -1;
        this.records = new HashMap<>();
    }

    public long write(long position, ByteBuffer buffer){
        if (! records.isEmpty()){
//            Data last = cache.apply(active.getCapacity());
//            if (last == null){
//                last = new SSD(fw, active.getPosition(), active.getCapacity());
//            }else{
//                last.set(active.get());
//            }
            Data last = new SSD(fw, active.getPosition(), active.getCapacity());
            records.put(offset, last);
        }
        ++ offset;
        active.set(buffer);
        active.setPosition(position);
        records.put(offset, active);
        return offset;
    }

    public List<ByteBuffer> read(long offset, int num){
        List<ByteBuffer> buffers = new ArrayList<>();
        for (long i = offset; i < offset + num; i ++){
            Data data = records.get(offset);
            if (data == null){
                break;
            }
            buffers.add(data.get());
            cache.recycle(data);
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
