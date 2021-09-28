package io.openmessaging.mq;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Barrier {

    private long position;

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
//                    aof.force();
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
            action.run();
        }
    }

    public synchronized long write(ByteBuffer... buffers){
        for (ByteBuffer buffer: buffers){
            position += buffer.capacity();
            this.buffers.add(buffer);
        }
        return position;
    }

    public synchronized ByteBuffer[] getAndClear(){
        ByteBuffer[] arr = this.buffers.toArray(EMPTY);
        this.buffers.clear();
        return arr;
    }

}
