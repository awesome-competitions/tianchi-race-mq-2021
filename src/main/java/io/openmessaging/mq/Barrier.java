package io.openmessaging.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class Barrier {

    private final Runnable action;

    private long position;

    private final List<ByteBuffer> buffers;

    private final List<Threads.Context> contexts;

    private final CyclicBarrier barrier;

    private final FileWrapper aof;

    public final static ByteBuffer[] EMPTY = new ByteBuffer[0];

    public Barrier(int parties, FileWrapper aof) {
        this.buffers = new ArrayList<>();
        this.contexts = new ArrayList<>();
        this.aof = aof;
        this.action = ()->{
            ByteBuffer[] bs = new ByteBuffer[contexts.size()];
            for(int i = 0; i < contexts.size(); i ++){
                bs[i] = contexts.get(i).getBuffer();
            }
            try {
                aof.write(buffers.toArray(EMPTY));
                aof.force();
                for (ByteBuffer b: bs){
                    b.clear();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

//            long pos = 0;
//            ByteBuffer[] bs = new ByteBuffer[contexts.size()];
//            for(int i = 0; i < contexts.size(); i ++){
//                bs[i] = contexts.get(i).getBuffer();
//            }
//            for (Threads.Context ctx: contexts){
//                if (ctx.isReadyWrite()){
//                    buffers.add(ctx.getBuffer());
//                    ctx.setSsdPos(pos);
//                    pos += ctx.getBuffer().limit();
//                    ctx.setReadyWrite(false);
//                }
//            }
//            if (buffers.size() > 0){
//                try {
//                    position = aof.write(buffers.toArray(EMPTY));
//                    aof.force();
//
//                    buffers.forEach(ByteBuffer::clear);
//                    buffers.clear();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
        };
        this.barrier = new CyclicBarrier(parties, this.action);
    }

    public long await(long timeout, TimeUnit unit){
        try {
            this.barrier.await(timeout, unit);
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            e.printStackTrace();
            try {
                long pos = aof.write(Threads.get().getBuffer());
                aof.force();
                return pos;
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
        return -1;
    }

    public synchronized void register(Threads.Context ctx){
        contexts.add(ctx);
    }

    public long getPosition() {
        return position;
    }
}
