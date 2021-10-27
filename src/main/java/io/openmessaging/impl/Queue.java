package io.openmessaging.impl;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class Queue {

    private long offset;

    private boolean reading;

    private final List<Data> records;

    public Queue() {
        this.offset = -1;
        this.records = new ArrayList<>(30);
    }

    public long nextOffset(){
        return ++ offset;
    }

    public void write(Aof aof, long position, ByteBuffer buffer){
        Threads.Context ctx = Threads.get();
        Data data = ctx.allocateReadBuffer(buffer.limit());
        if (data == null){
            data = ctx.allocatePMem(buffer.limit());
        }
        if (data != null){
            data.set(buffer);
            records.add(data);
            return;
        }
        records.add(new SSD(aof, position, buffer.limit()));
    }

    public Map<Integer, ByteBuffer> read(long offset, int num) {
        Threads.Context ctx = Threads.get();
        if (!reading){
            ctx.getPools().execute(()->{
                for (long i = 0; i < offset; i ++){
                    recycleData(ctx, records.get((int) i));
                }
            });
            reading = true;
        }

        long nextReadOffset = (int) Math.min(offset + num, records.size());
        int size = (int) (nextReadOffset - offset);
        ResultMap results = ctx.getResults();
        results.setMaxIndex(size - 1);

        // 预加载
        preloading(ctx, nextReadOffset);

        // 读当前
        Semaphore semaphore = ctx.getSemaphore();
        for (int i = (int) offset; i < nextReadOffset; i ++){
            Data data = records.get(i);
            int index = (int) (i - offset);
            ctx.getPools().execute(()->{
                results.put(index, data.get(ctx));
                recycleData(ctx, data);
                semaphore.release();
            });
        }
        try {
            semaphore.acquire(size);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return results;
    }

    private void preloading(Threads.Context ctx, long nextReadOffset){
        long nextLoadSize = Math.min(this.offset - nextReadOffset + 1, 8);
        for (int i = (int) nextReadOffset; i < nextReadOffset + nextLoadSize; i ++){
            if (i >= records.size()){
                break;
            }
            int index = i;
            Data data = records.get(index);
            if (data.isSSD()){
                ctx.getPools().execute(()->{
                    ByteBuffer buffer = data.get(ctx);
                    Data bufferData = ctx.allocatePMem(buffer.limit());
                    if (bufferData != null){
                        bufferData.set(buffer);
                        records.set(index, bufferData);
                    }
                });
            }
        }
    }

    private void recycleData(Threads.Context ctx, Data data){
        if (data.isDram()){
            ctx.recycleReadBuffer(data);
        }else if (data.isPMem()){
            ctx.recyclePMem(data);
        }
    }

    public List<Data> getRecords() {
        return records;
    }

}
