package io.openmessaging.mq;


import java.nio.ByteBuffer;
import java.util.*;

public class Queue {

    private long offset;

    private final Map<Long, Data> records;

    private final Cache cache;

    private final FileWrapper aof;

    private boolean reading;

    private long nextReadOffset;

    public Queue(FileWrapper aof, Cache cache) {
        this.cache = cache;
        this.aof = aof;
        this.offset = -1;
        this.records = new HashMap<>();
        Monitor.queueCount ++;
    }

    public long nextOffset(){
        return ++ offset;
    }

    public boolean write(long position, ByteBuffer buffer){
        Data data = cache.allocate(buffer.limit());
        if(data != null){
            data.set(buffer);
            records.put(offset, data);
            return true;
        }
        records.put(offset, new SSD(aof, position, buffer.limit()));
        return false;
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
        for (long i = offset; i < offset + num; i ++){
            Data data = records.get(i);
            if (data == null){
                break;
            }
            if (data instanceof PMem){
                buffers.add(data.get());
                cache.recycle(data);
            }else if (data instanceof SSD){
                buffers.add(data.get());
            }else if (data instanceof Dram){
                buffers.add(data.get());
            }
        }
        nextReadOffset = offset + buffers.size();
        return buffers;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getNextReadOffset() {
        return nextReadOffset;
    }

    public Map<Long, Data> getRecords() {
        return records;
    }
}
