package io.openmessaging.mq;

import io.openmessaging.utils.CollectionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

public class Barrier {

    private final Runnable action;

    private final List<ByteBuffer> buffers;

    private final CyclicBarrier barrier;

    public final static ByteBuffer[] EMPTY = new ByteBuffer[0];

    public Barrier(int parties, FileWrapper aof) {
        this.buffers = new ArrayList<>();
        this.action = ()->{
            ByteBuffer[] array = getAndClear();
            if (array.length > 0){
                try {
                    aof.write(array);
                    aof.force();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        this.barrier = new CyclicBarrier(parties, this.action);
    }

    public int await(long timeout, TimeUnit unit){
        try {
            return this.barrier.await(timeout, unit);
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            e.printStackTrace();
            action.run();
        }
        return -1;
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
