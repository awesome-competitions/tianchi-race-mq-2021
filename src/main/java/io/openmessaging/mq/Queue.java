package io.openmessaging.mq;

import io.openmessaging.consts.Const;
import io.openmessaging.utils.BufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;

public class Queue {

    private long offset;

    private final List<Data> records;

    private boolean reading;

    private long nextReadOffset;

    public Queue() {
        this.offset = -1;
        this.records = new ArrayList<>(30);
        Monitor.queueCount ++;
    }

    public long nextOffset(){
        return ++ offset;
    }

    public void write(FileWrapper aof, long position, ByteBuffer buffer, Data pMem){
        if (pMem != null){
            records.add(pMem);
            return;
        }
        Threads.Context ctx = Threads.get();
        Data data = ctx.allocateReadBuffer(buffer.limit());
        if (data == null){
            Monitor.missingDramSize ++;
            data = Buffers.allocateReadBuffer(buffer.limit());
        }
        if (data != null){
            data.set(buffer);
            records.add(data);
            return;
        }
        Monitor.readSSDCount ++;
        records.add(new SSD(aof, position, buffer.limit()));
    }

    public Map<Integer, ByteBuffer> read(long offset, int num) throws IOException {
        Threads.Context ctx = Threads.get();
        if (!reading){
            for (long i = 0; i < offset; i ++){
                Data data = records.get((int) i);
                if (data.isPMem()){
                    ctx.recycleReadBuffer(data);
                }
            }
            reading = true;
        }

        nextReadOffset = (int) Math.min(offset + num, records.size());
        int size = (int) (nextReadOffset - offset);
        Map<Integer, ByteBuffer> results = ctx.getResults();

        MappedByteBuffer[] mappedByteBuffers = ctx.getMappedByteBuffers();
        MappedByteBuffer tmp;
        for (int i = 0; i <= ((ArrayMap) results).getMaxIndex(); i ++){
            tmp = mappedByteBuffers[i];
            if (tmp != null){
                BufferUtils.clean(tmp);
                mappedByteBuffers[i] = null;
            }
        }
        ((ArrayMap) results).setMaxIndex(size - 1);
        Semaphore semaphore = ctx.getSemaphore();
        for (int i = (int) offset; i < nextReadOffset; i ++){
            Data data = records.get(i);
            int index = (int) (i - offset);
            ctx.getPools().execute(()->{
                try {
                    if (data.isPMem()){
                        results.put(index, data.get(ctx));
                        ctx.recyclePMem(data);
                    }else if (data.isDram()){
                        results.put(index, data.get(ctx));
                        ctx.recycleReadBuffer(data);
                    }else {
                        SSD ssd = ((SSD) data);
                        MappedByteBuffer mappedByteBuffer = ssd.getChannel().map(FileChannel.MapMode.READ_ONLY, ssd.getPosition() + 9, ssd.getCapacity());
                        mappedByteBuffers[index] = mappedByteBuffer;
                        results.put(index, mappedByteBuffer);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    semaphore.release();
                }
            });
        }
        try {
            semaphore.acquire(size);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return results;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public List<Data> getRecords() {
        return records;
    }

}
