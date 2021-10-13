package io.openmessaging.mq;

import io.openmessaging.consts.Const;
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

    private long position;

    private final CyclicBarrier barrier;

    private final FileWrapper aof;

    private final ByteBuffer block;

    private final Block aep;

    public Barrier(int parties, FileWrapper aof, Block aep) {
        this.aof = aof;
        this.aep = aep;
        this.block = ByteBuffer.allocateDirect((int) (Const.K * 1024));
        this.barrier = new CyclicBarrier(parties, ()->{
            try {
                block.flip();
                position = aof.writeWithoutSync(block);
                aof.force();

                aep.getFw().getChannel().transferFrom(aof.getChannel(), position, block.limit());

                block.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void await(long timeout, TimeUnit unit) throws BrokenBarrierException {
        try {
            this.barrier.await(timeout, unit);
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            throw new BrokenBarrierException();
        }
    }

    public synchronized long write(int topic, int queueId, long offset, ByteBuffer buffer){
        long pos = block.position();
        block.put((byte) topic)
                .putShort((short) queueId)
                .putInt((int) offset)
                .putShort((short) buffer.limit())
                .put(buffer);
        return pos;
    }

    public long writeAndFsync(ByteBuffer buffer){
        try {
            return aof.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public long getPosition() {
        return position;
    }

    public FileWrapper getAof() {
        return aof;
    }
}
