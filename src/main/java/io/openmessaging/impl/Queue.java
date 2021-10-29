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
//            if (data != null){
//                ByteBuffer aepBuffer = ctx.getAepBuffers().poll();
//                if (aepBuffer == null){
//                    aepBuffer = ByteBuffer.allocateDirect(Const.PROTOCOL_DATA_MAX_SIZE);
//                }
//                aepBuffer.put(buffer);
//                aepBuffer.flip();
//                records.add(new SSD(aof, position, buffer.limit()));
//
//                ByteBuffer finalByteBuffer = aepBuffer;
//                Data finalData = data;
//                long finalOffset = offset;
//                ctx.getPools().execute(()->{
//                    finalData.set(finalByteBuffer);
//                    finalByteBuffer.clear();
//                    records.set((int) finalOffset, finalData);
//                    ctx.getAepBuffers().add(finalByteBuffer);
//                });
//                return;
//            }
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

        // 读当前
        Semaphore semaphore = ctx.getSemaphore();
        for (int i = (int) offset; i < nextReadOffset; i ++){
            Data data = records.get(i);
            int index = (int) (i - offset);
            ctx.getPools().execute(()->{
                results.put(index, data.get(ctx.allocateBuffer(index)));
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
