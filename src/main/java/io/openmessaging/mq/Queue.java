package io.openmessaging.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class Queue {

    private long offset;

    private final Topic topic;

    private final Map<Long, Data> records;

    private final Cache cache;

    private SSD ssd;

    private boolean reading;

    private final static Logger LOGGER = LoggerFactory.getLogger(Queue.class);

    public Queue(Topic topic, Cache cache) {
        this.topic = topic;
        this.cache = cache;
        this.offset = -1;
        this.records = new HashMap<>();
    }

    public long nextOffset(){
        return ++ offset;
    }

    public long write(ByteBuffer buffer){
        Data data = cache.allocate(buffer.limit());
        if(data != null){
            data.set(buffer);
            records.put(offset, data);
            return offset;
        }
        if (ssd == null || ! ssd.writable(buffer)){
            ssd = topic.nextSSDBlock();
        }
        ssd.write(offset, buffer);
        records.put(offset, ssd);
        return offset;
    }

    public List<ByteBuffer> read(long offset, int num){
        if (!reading){
            new Thread(()->{
                for (long i = 0; i < offset; i ++){
                    Data data = records.remove(i);
                    cache.recycle(data);
                }
            }).start();
            reading = true;
        }
        List<ByteBuffer> buffers = new ArrayList<>();
        Map<Long, Data> tmpRecords = new HashMap<>();
        for (long i = offset; i < offset + num; i ++){
            Data data = tmpRecords.remove(i);
            if (data == null){
                data = records.get(i);
                if (data == null){
                    break;
                }
            }
            if (data instanceof PMem){
                buffers.add(data.get());
                cache.recycle(data);
            }else if (data instanceof SSD){
                SSD ssdBlock = (SSD) data;
                tmpRecords.putAll(ssdBlock.load());
                buffers.add(tmpRecords.remove(i).get());
            }else if (data instanceof Dram){
                buffers.add(data.get());
            }
        }
        long nextReadOffset = offset + buffers.size();
        if (! tmpRecords.isEmpty()){
            for (Map.Entry<Long, Data> tmpRecord: tmpRecords.entrySet()){
                if (tmpRecord.getKey() < nextReadOffset){
                    continue;
                }
                Data data = cache.allocate(tmpRecord.getValue().getCapacity());
                if (data != null){
                    data.set(tmpRecord.getValue().get());
                    records.put(tmpRecord.getKey(), data);
                }
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
}
