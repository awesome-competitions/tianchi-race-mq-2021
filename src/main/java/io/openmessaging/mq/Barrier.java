package io.openmessaging.mq;

import io.openmessaging.consts.Const;
import io.openmessaging.utils.BufferUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Barrier {

    private long position;

    private long aepPosition;

    private final CyclicBarrier barrier;

    private final FileWrapper aof;

    private final ByteBuffer block;

    private final Block aep;

    private boolean writeAep;

    private static final ExecutorService ES = Executors.newFixedThreadPool(10);

    public Barrier(int parties, FileWrapper aof, Block aep) {
        this.aof = aof;
        this.aep = aep;
        this.block = ByteBuffer.allocateDirect((int) (Const.K * 256));
        this.barrier = new CyclicBarrier(parties, ()->{
            try {
                block.flip();
                position = aof.writeWithoutSync(block);
                aof.force();

                aepPosition = aep.allocate(block.limit());
                writeAep = aepPosition != -1;
                if (writeAep){
                    block.flip();
                    ByteBuffer blockBak = ByteBuffer.allocateDirect((int) (Const.K * 256));
                    blockBak.put(block);
                    blockBak.flip();
                    ES.execute(()->{
                        try {
                            aep.getFw().write(aepPosition, blockBak);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }finally {
                            BufferUtils.clean(blockBak);
                        }
                    });

                }

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
            clearBlock();
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

    public long writeAndFsync(int topic, int queueId, long offset, ByteBuffer buffer){
        try {
            ByteBuffer data = ByteBuffer.allocate((int) (Const.K * 17 + 9));
            data.put((byte) topic)
                    .putShort((short) queueId)
                    .putInt((int) offset)
                    .putShort((short) buffer.limit())
                    .put(buffer);
            data.flip();
            long pos = aof.write(data);
            aof.force();
            return pos;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public void clearBlock(){
        block.clear();
    }

    public long getPosition() {
        return position;
    }

    public FileWrapper getAof() {
        return aof;
    }

    public long getAepPosition() {
        return aepPosition;
    }

    public boolean isWriteAep() {
        return writeAep;
    }

    public Block getAep() {
        return aep;
    }
}
