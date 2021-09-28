package io.openmessaging.mq;

import java.nio.ByteBuffer;
import java.util.*;

public class Queue {

    private long offset;

    private Data active;

    private final Map<Long, Data> records;

    private final FileWrapper fw;

    private boolean reading;

    public Queue(FileWrapper fw) {
        this.fw = fw;
        this.offset = -1;
        this.records = new HashMap<>();
    }

    public long write(long position, ByteBuffer buffer){
        if (! reading){
            active.setPosition(position);
            active.setCapacity(buffer.capacity());
            records.put(++ offset, new SSD(fw, position, buffer.capacity()));
            return offset;
        }
        if (! records.isEmpty()){
            Data last = new SSD(fw, active.getPosition(), active.getCapacity());
            records.put(offset, last);
        }
        active.set(buffer);
        active.setPosition(position);
        records.put(++ offset, active);
        return offset;
    }

    public List<ByteBuffer> read(long offset, int num){
        reading = true;
        List<ByteBuffer> buffers = new ArrayList<>();
        for (long i = offset; i < offset + num; i ++){
            Data data = records.get(offset);
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
