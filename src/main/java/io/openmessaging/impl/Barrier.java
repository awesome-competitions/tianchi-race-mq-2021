package io.openmessaging.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

public class Barrier {

    private boolean aepEnable;

    private long ssdPosition;

    private long aepPosition;

    private final Aep aep;

    private final Aof aof;

    private final ByteBuffer buffer;

    private final CyclicBarrier barrier;

    public final LinkedBlockingQueue<ByteBuffer> aepBuffers = new LinkedBlockingQueue<>();

    public final ThreadPoolExecutor pools = (ThreadPoolExecutor) Executors.newFixedThreadPool(30);

    public Barrier(int parties, Aof aof, Aep aep, Buffers buffers) {
        this.aof = aof;
        this.aep = aep;
        this.buffer = ByteBuffer.allocateDirect(Const.AOF_FLUSHED_BUFFER_SIZE);
        this.barrier = new CyclicBarrier(parties, ()->{
            try {
                int dif = (int) (Const.K_4 - buffer.position() % Const.K_4);
                if (dif <= Const.PROTOCOL_HEADER_SIZE) dif += Const.K_4;

                int oldPos = buffer.position();
                short fillDataSize = (short) (dif - Const.PROTOCOL_HEADER_SIZE);
                buffer.put((byte) 127).putShort((short) 1).putInt(1).putShort(fillDataSize);
                buffer.position(buffer.position() + fillDataSize);
                buffer.flip();

                ssdPosition = aof.writeWithoutSync(buffer);
                aof.force();

                if (! buffers.completed()){
                    buffer.flip();
                    buffer.limit(oldPos);
                    aepPosition = aep.allocatePos(buffer.limit());
                    if (aepEnable = aepPosition != -1){
                        writeAep(aepPosition, buffer);
                    }
                }
                buffer.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private void writeAep(long position, ByteBuffer buffer){
        ByteBuffer aepBuffer = aepBuffers.poll();
        if (aepBuffer == null){
            aepBuffer = ByteBuffer.allocateDirect(Const.AOF_FLUSHED_BUFFER_SIZE);
        }
        aepBuffer.put(buffer).flip();
        ByteBuffer finalBlock = aepBuffer;
        pools.execute(()->{
            aep.write(position, finalBlock);
            finalBlock.clear();
            aepBuffers.add(finalBlock);
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

    public synchronized long write(int topic, int queueId, long offset, ByteBuffer data){
        long pos = buffer.position();
        buffer.put((byte) topic)
                .putShort((short) queueId)
                .putInt((int) offset)
                .putShort((short) data.limit())
                .put(data);
        return pos;
    }

    public long writeAndFsync(int topic, int queueId, long offset, ByteBuffer buffer){
        try {
            ByteBuffer data = ByteBuffer.allocate(Const.PROTOCOL_MAX_SIZE);
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
        buffer.clear();
    }

    public long getSsdPosition() {
        return ssdPosition;
    }

    public Aof getAof() {
        return aof;
    }

    public long getAepPosition() {
        return aepPosition;
    }

    public Aep getAep() {
        return aep;
    }

    public boolean aepEnable() {
        return aepEnable;
    }

}
