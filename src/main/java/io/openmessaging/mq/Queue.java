package io.openmessaging.mq;


import java.nio.ByteBuffer;
import java.util.*;

public class Queue {

    private long offset;

    private final Map<Long, Data> records;

    private final Cache cache;

    private final FileWrapper aof;

    private boolean reading;

    public Queue(FileWrapper aof, Cache cache) {
        this.cache = cache;
        this.aof = aof;
        this.offset = -1;
        this.records = new HashMap<>();
    }

    public long nextOffset(){
        return ++ offset;
    }

    public long write(long position, ByteBuffer buffer){
        Data data = cache.allocate(buffer.limit());
        if(data != null){
            data.set(buffer);
            records.put(offset, data);
            return offset;
        }
        records.put(offset, new SSD(aof, position, buffer.limit()));
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
        return buffers;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
