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
        Data data = Threads.get().allocateReadBuffer();
        if (data == null){
            data = Buffers.allocateReadBuffer();
            if (data == null){
                data = cache.allocate(buffer.limit());
                if (data == null){
                    if (reading){
                        data = Buffers.allocateExtraData();
                    }
                }
            }
        }
        if (data != null){
            data.set(buffer);
        }else{
            data = new SSD(aof, position, buffer.limit());
        }
        records.add(data);
        return false;
    }

    public List<ByteBuffer> read(long offset, int num){
        Threads.Context ctx = Threads.get();
        if (!reading){
            for (long i = 0; i < offset; i ++){
                Data data = records.get((int) i);
                if (data instanceof PMem){
                    ctx.recyclePMem(data);
                }else if (data instanceof Dram){
                    ctx.recycleReadBuffer(data);
                }
            }
            reading = true;
        }
        List<ByteBuffer> buffers = new ArrayList<>();
        int end = (int) Math.min(offset + num, records.size());
        for (int i = (int) offset; i < end; i ++){
            Data data = records.get(i);
            if (data instanceof PMem){
                buffers.add(data.get());
                ctx.recyclePMem(data);
            }else if (data instanceof SSD){
                buffers.add(data.get());
            }else if (data instanceof Dram){
                buffers.add(data.get());
                ctx.recycleReadBuffer(data);
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
