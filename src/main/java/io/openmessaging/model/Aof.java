package io.openmessaging.model;

import io.openmessaging.consts.Const;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Aof {

    private final FileWrapper wrapper;

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition cond = lock.newCondition();

    private final int maxCount;

    private final long maxSize;

    private int count;

    private long size;

    private final AtomicInteger version;

    private ByteBuffer buffer;

    public Aof(FileWrapper wrapper, Config config) {
        this.wrapper = wrapper;
        this.maxCount = config.getMaxCount();
        this.maxSize = config.getMaxSize();
        this.version = new AtomicInteger();
        this.buffer = ByteBuffer.allocate((int) (Const.K * 64));
    }

    public void write(ByteBuffer data) throws IOException {
        try {
            lock.lock();
            int v = this.version.get();
            count ++;
            size += data.capacity();

            if (buffer.remaining() >= data.capacity()){
                buffer.put(data);
            }else{
                data.limit(buffer.remaining());
                buffer.put(data);
                data.limit(data.capacity());
            }
            if (buffer.remaining() == 0){
                this.wrapper.getChannel().write(buffer);
                buffer.clear();
                if (data.remaining() > 0){
                    buffer.put(data);
                }
            }
            if (maxSize <= size || count == maxCount){
                next(v);
                return;
            }
            long nanos = this.cond.awaitNanos(TimeUnit.SECONDS.toNanos(10));
            if (nanos <= 0){
                next(v);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    private void next(int v) throws IOException, InterruptedException {
        if (! version.compareAndSet(v, v + 1)){
            return;
        }
        this.count = 0;
        this.size = 0;
        if (buffer.remaining() > 0){
            this.wrapper.getChannel().write(buffer);
            buffer.clear();
        }
        this.wrapper.getChannel().force(false);
        this.cond.signalAll();
    }

}
