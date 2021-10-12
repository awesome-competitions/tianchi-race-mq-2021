package io.openmessaging.mq;


import java.nio.ByteBuffer;
import java.util.*;

public class Queue {

    private long offset;

    private final List<Data> records;

    private final Cache cache;

    private boolean reading;

    public Queue(Cache cache) {
        this.cache = cache;
        this.offset = -1;
        this.records = new ArrayList<>(200);
        Monitor.queueCount ++;
    }

    public long nextOffset(){
        return ++ offset;
    }

    public boolean write(FileWrapper aof, long position, ByteBuffer buffer){
        Data data = Buffers.allocateReadBuffer();
        if (data != null){
            data.set(buffer);
            records.add(data);
            return true;
        }
        data = cache.allocate(buffer.limit());
        if(data != null){
            data.set(buffer);
            records.add(data);
            return true;
        }
        if (reading){
            data = Buffers.allocateExtraData();
            data.set(buffer);
            return true;
        }
        records.add(new SSD(aof, position, buffer.limit()));
        return false;
    }

    public List<ByteBuffer> read(long offset, int num){
        if (!reading){
            new Thread(()->{
                for (long i = 0; i < offset; i ++){
                    Data data = records.get((int) i);
                    if (data instanceof PMem){
                        cache.recycle(data);
                    }else if (data instanceof Dram){
                        Buffers.recycle(data);
                    }
                }
            }).start();
            reading = true;
        }
        List<ByteBuffer> buffers = new ArrayList<>();
        int end = (int) Math.min(offset + num, records.size());
        for (int i = (int) offset; i < end; i ++){
            Data data = records.get(i);
            if (data instanceof PMem){
                buffers.add(data.get());
                cache.recycle(data);
            }else if (data instanceof SSD){
                buffers.add(data.get());
            }else if (data instanceof Dram){
                buffers.add(data.get());
                Buffers.recycle(data);
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
