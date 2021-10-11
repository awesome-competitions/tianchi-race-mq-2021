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

    private long position;

    private final List<Threads.Context> contexts;

    private final CyclicBarrier barrier;

    private final FileWrapper aof;

    public final ByteBuffer[] buffers;

    public Barrier(int parties, FileWrapper aof) {
        this.contexts = new ArrayList<>();
        this.aof = aof;
        this.buffers = new ByteBuffer[parties];
        this.barrier = new CyclicBarrier(parties, ()->{
            long pos = 0;
            for(int i = 0; i < contexts.size(); i ++){
                Threads.Context ctx = contexts.get(i);
                buffers[i] = ctx.getBuffer();
                ctx.setSsdPos(pos);
                pos += ctx.getBuffer().limit();
            }
            try {
                position = aof.write(buffers);
                aof.force();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public long await(long timeout, TimeUnit unit){
        try {
            this.barrier.await(timeout, unit);
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            e.printStackTrace();
            try {
                ByteBuffer buffer = Threads.get().getBuffer();
                long pos = aof.write(buffer);
                aof.force();
                buffer.clear();
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

    public FileWrapper getAof() {
        return aof;
    }
}
