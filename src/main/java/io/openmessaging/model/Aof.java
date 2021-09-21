package io.openmessaging.model;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Aof {

    private FileWrapper wrapper;

    private CyclicBarrier cyclicBarrier;

    public Aof(FileWrapper wrapper, Config config){
        this.wrapper = wrapper;
        this.cyclicBarrier = new CyclicBarrier(config.getBatchSize(), this::force);
    }

    public synchronized void write(int topicId, int queueId, byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(5 + bytes.length)
                .put((byte) topicId)
                .putShort((short) queueId)
                .putShort((short) bytes.length)
                .put(bytes);
        buffer.flip();
        this.wrapper.getFileChannel().write(buffer);
    }

    public void force() {
        try {
            this.wrapper.getFileChannel().force(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void await(){
        try {
            this.cyclicBarrier.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            force();
        }
    }

}
