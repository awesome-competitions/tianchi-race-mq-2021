package io.openmessaging.mq;

import java.nio.ByteBuffer;
import java.util.*;

public class Queue {

    private long offset;

    private Data active;

    private Data last;

    private final Map<Long, Data> records;

    private Cache cache;

    private final FileWrapper fw;

    private boolean reading;

    public Queue(Cache cache, FileWrapper fw) {
        this.fw = fw;
        this.cache = cache;
        this.offset = -1;
        this.records = new HashMap<>();
    }

    public long write(long position, ByteBuffer buffer){
        Data data = cache.apply(buffer.capacity());
        if(data != null){
            data.set(buffer);
            records.put(++ offset, data);
            return offset;
        }
        if (! reading){
            records.put(++ offset, new SSD(fw, position, buffer.capacity()));
            return offset;
        }
        if (last != null){
            records.put(offset, last);
        }
        active.set(buffer);
        records.put(++ offset, active);
        last = new SSD(fw, active.getPosition(), active.getCapacity());
        return offset;
    }

    public List<ByteBuffer> read(long offset, int num){
        reading = true;
        List<ByteBuffer> buffers = new ArrayList<>();
        for (long i = offset; i < offset + num; i ++){
            Data data = records.get(i);
            if (data == null){
                break;
            }
            buffers.add(data.get());
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

    public Map<Long, Data> getRecords() {
        return records;
    }

    public Data getActive() {
        return active;
    }
}
