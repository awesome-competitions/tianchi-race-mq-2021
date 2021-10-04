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

    private AtomicInteger count;

    private final List<ByteBuffer> buffers;

    private CyclicBarrier barrier;

    private final ReentrantLock lock = new ReentrantLock();

    public final static ByteBuffer[] EMPTY = new ByteBuffer[0];

    private final static Logger LOGGER = LoggerFactory.getLogger(Barrier.class);

    public Barrier(int parties, FileWrapper aof) {
        this.buffers = new ArrayList<>();
        this.count = new AtomicInteger();
        this.action = ()->{
            ByteBuffer[] array = getAndClear();
            if (array.length > 0){
                try {
                    aof.write(array);
                    aof.force();
                    count.set(0);
                    Arrays.stream(array).forEach(ByteBuffer::clear);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        this.barrier = new CyclicBarrier(parties, this.action);
    }

    public void await(long timeout, TimeUnit unit){
        try {
            count.incrementAndGet();
            this.barrier.await(timeout, unit);
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            try{
                lock.lock();
                if (count.get() > 0){
                    LOGGER.info("barrier drop, count {}", count);
                    this.barrier = new CyclicBarrier(count.get(), this.action);
                    action.run();
                }
            }finally {
                lock.unlock();
            }
        }
    }

    public synchronized void write(ByteBuffer... buffers){
        this.buffers.addAll(Arrays.asList(buffers));
    }

    public synchronized ByteBuffer[] getAndClear(){
        ByteBuffer[] arr = this.buffers.toArray(EMPTY);
        this.buffers.clear();
        return arr;
    }

}
