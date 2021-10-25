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

    public Queue() {
        this.offset = -1;
        this.records = new ArrayList<>(30);
        Monitor.queueCount ++;
    }

    public long nextOffset(){
        return ++ offset;
    }

    public Data allocateData(ByteBuffer buffer){
        Threads.Context ctx = Threads.get();
        Data data = ctx.allocateReadBuffer(buffer.limit());
        if (data == null){
            Monitor.missingDramSize ++;
            data = ctx.allocatePMem(buffer.limit());
            if (data == null){
                data = Buffers.allocateReadBuffer(buffer.limit());
            }else{
                ByteBuffer byteBuffer = ctx.getAepBuffers().poll();
                if (byteBuffer == null){
                    byteBuffer = ByteBuffer.allocateDirect((int) (Const.K * 17));
                }
                byteBuffer.put(buffer);
                byteBuffer.flip();
                ctx.getAepTasks().add(new AepData(data, byteBuffer, offset, records));
            }
        }
        return data;
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
            data = ctx.allocatePMem(buffer.limit());
            if (data == null){
                data = Buffers.allocateReadBuffer(buffer.limit());
            }else{
                ByteBuffer byteBuffer = ctx.getAepBuffers().poll();
                if (byteBuffer == null){
                    byteBuffer = ByteBuffer.allocateDirect((int) (Const.K * 17));
                }
                byteBuffer.put(buffer);
                byteBuffer.flip();
                records.add(new SSD(aof, position, buffer.limit()));
                ctx.getAepTasks().add(new AepData(data, byteBuffer, offset, records));
                return;
            }
        }
        if (data != null){
            data.set(buffer);
            records.add(data);
            return;
        }
        records.add(new SSD(aof, position, buffer.limit()));
    }

    public Map<Integer, ByteBuffer> read(long offset, int num) throws IOException {
        Threads.Context ctx = Threads.get();
        if (!reading){
            for (long i = 0; i < offset; i ++){
                Data data = records.get((int) i);
                if (data.isDram()){
                    ctx.recycleReadBuffer(data);
                }else if (data.isPMem()){
                    ctx.recyclePMem(data);
                }
            }
            reading = true;
        }

        long nextReadOffset = (int) Math.min(offset + num, records.size());
        int size = (int) (nextReadOffset - offset);
        FutureMap results = ctx.getResults();
        results.setMaxIndex(size - 1);

        Semaphore semaphore = ctx.getSemaphore();
        for (int i = (int) offset; i < nextReadOffset; i ++){
            Data data = records.get(i);
            int index = (int) (i - offset);
            ctx.getPools().execute(()->{
                try {
                    results.put(index, data.get(ctx));
                    if (data.isDram()){
                        ctx.recycleReadBuffer(data);
                    }else if (data.isPMem()){
                        ctx.recyclePMem(data);
                    }
                } finally {
                    semaphore.release();
                }
            });
        }
        long nextLoadSize;
        int ssdSize = 0;
        if (offset > nextReadOffset){
            nextLoadSize = Math.min(offset - nextReadOffset + 1, 20);
            for (int i = (int) nextReadOffset; i < nextReadOffset + nextLoadSize; i ++){
                Data data = records.get(i);
                if (data.isSSD()){
                    ssdSize ++;
                }
            }
            for (int i = (int) nextReadOffset; i < nextReadOffset + nextLoadSize; i ++){
                Data data = records.get(i);
                int index = (int) (i - offset);
                if (data.isSSD()){
                    ctx.getPools().execute(()->{
                        try {
                            ByteBuffer buffer = data.get(ctx);
                            Data bufferData = allocateData(buffer);
                            if (bufferData != null){
                                records.set(index, bufferData);
                            }
                        } finally {
                            semaphore.release();
                        }
                    });
                }
            }
        }
        try {
            semaphore.acquire(size + ssdSize);
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
