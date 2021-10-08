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

    private final Loader loader;

    public final static ByteBuffer[] EMPTY = new ByteBuffer[0];

    public Barrier(int parties, FileWrapper aof, Loader loader) {
        this.buffers = new ArrayList<>();
        this.contexts = new ArrayList<>();
        this.aof = aof;
        this.action = ()->{
            ByteBuffer[] bs = new ByteBuffer[contexts.size()];
            long pos = 0;
            for(int i = 0; i < contexts.size(); i ++){
                Threads.Context ctx = contexts.get(i);
                bs[i] = ctx.getBuffer();
                ctx.setSsdPos(pos);
            }
            try {
                position = aof.write(bs);
                aof.force();
                for (ByteBuffer b: bs){
                    b.clear();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
        this.barrier = new CyclicBarrier(parties, this.action);
        this.loader = loader;
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

    public Loader getLoader() {
        return loader;
    }

    public FileWrapper getAof() {
        return aof;
    }
}
