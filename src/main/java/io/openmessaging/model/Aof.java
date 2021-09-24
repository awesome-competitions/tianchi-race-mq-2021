package io.openmessaging.model;

import io.openmessaging.consts.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private int count;

    private final AtomicInteger version;

    private final List<ByteBuffer> buffers;

    private final static Logger LOGGER = LoggerFactory.getLogger(Aof.class);

    private final static ByteBuffer[] EMPTY = new ByteBuffer[0];

    public Aof(FileWrapper wrapper, Config config) {
        this.wrapper = wrapper;
        this.maxCount = config.getMaxCount();
        this.version = new AtomicInteger();
        this.buffers = new ArrayList<>();
    }

    public FileWrapper getWrapper() {
        return wrapper;
    }

    public void write(ByteBuffer data) throws IOException {
        try {
            lock.lock();
            int v = this.version.get();
            count ++;
            buffers.add(data);
            if (count == maxCount){
                next(-1);
                return;
            }
            long nanos = this.cond.awaitNanos(TimeUnit.SECONDS.toNanos(10));
            if (nanos <= 0){
                LOGGER.info("blocked count: {}", count);
                next(v);
                return;
            }
            lock.unlock();
        } catch (InterruptedException e) {
            e.printStackTrace();
            lock.unlock();
        }
    }

    private void next(int v) throws IOException, InterruptedException {
        if (v >= 0 && ! version.compareAndSet(v, v + 1)){
            return;
        }
        this.count = 0;
        ByteBuffer[] arr = buffers.toArray(EMPTY);
        buffers.clear();
        lock.unlock();
        this.wrapper.getChannel().write(arr);
        this.wrapper.getChannel().force(false);
        this.cond.signalAll();
    }

}
