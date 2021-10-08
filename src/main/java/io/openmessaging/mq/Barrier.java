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

    private long aos;

    private final List<ByteBuffer> buffers;

    private CyclicBarrier barrier;

    private final ReentrantLock lock = new ReentrantLock();

    public final static ByteBuffer[] EMPTY = new ByteBuffer[0];

    private final static Logger LOGGER = LoggerFactory.getLogger(Barrier.class);

    public Barrier(int parties, FileWrapper aof) {
        this.buffers = new ArrayList<>();
        this.action = ()->{
            ByteBuffer[] array = getAndClear();
            if (array.length > 0){
                try {
                    position = aof.write(array);
                    aof.force();
                    aos = 0;
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
            this.barrier.await(timeout, unit);
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public synchronized long write(ByteBuffer buffer){
        long oldAos = aos;
        this.buffers.add(buffer);
        aos += buffer.limit();
        return oldAos;
    }

    public synchronized ByteBuffer[] getAndClear(){
        ByteBuffer[] arr = this.buffers.toArray(EMPTY);
        this.buffers.clear();
        return arr;
    }

    public long getPosition() {
        return position;
    }
}
