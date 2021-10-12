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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Barrier {

    private AtomicLong position;

    private final CyclicBarrier barrier;

    private final FileWrapper aof;

    public Barrier(int parties, FileWrapper aof) {
        this.aof = aof;
        this.position = new AtomicLong();
        this.barrier = new CyclicBarrier(parties, ()->{
            try {
                aof.force();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void await(long timeout, TimeUnit unit){
        try {
            this.barrier.await(timeout, unit);
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            e.printStackTrace();
            try {
                aof.force();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    public long write(ByteBuffer buffer){
        try {
            return aof.getChannel().write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public FileWrapper getAof() {
        return aof;
    }
}
