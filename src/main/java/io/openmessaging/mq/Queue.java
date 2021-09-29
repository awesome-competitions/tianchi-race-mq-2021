package io.openmessaging.mq;

import java.nio.ByteBuffer;
import java.util.*;

public class Queue {

    private final int tid;

    private final int qid;

    private long offset;

    private Data active;

    private Data last;

    private final Map<Long, Data> records;

    private final Cache cache;

    private final FileWrapper fw;

    private boolean reading;

    public Queue(int tid, int qid, Cache cache, FileWrapper fw) {
        this.tid = tid;
        this.qid = qid;
        this.fw = fw;
        this.cache = cache;
        this.offset = -1;
        this.records = new HashMap<>();
    }

    public long write(long position, ByteBuffer buffer){
        ++ offset;
        Data data = cache.allocate(tid, qid, offset, position, buffer.limit());
        if(data != null){
            data.set(buffer);
            records.put(offset, data);
            return offset;
        }
        if (! reading){
            records.put(offset, new SSD(fw, position, buffer.limit()));
            return offset;
        }
        if (last != null){
            records.put(offset - 1, last);
        }
        active.set(buffer);
        records.put(++ offset, active);
        last = new SSD(fw, position, buffer.limit());
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
