package io.openmessaging.mq;

import io.openmessaging.consts.Const;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

public class Buffers {

    private static final LinkedBlockingQueue<ByteBuffer> buffers = new LinkedBlockingQueue<>();

    private static final LinkedBlockingQueue<Data> readBuffers = new LinkedBlockingQueue<>();

    static {
        initBuffers();
    }

    public static ByteBuffer allocateBuffer(){
        try {
            ByteBuffer buffer =  buffers.take();
            buffer.clear();
            buffers.add(buffer);
            return buffer;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return ByteBuffer.allocate((int) (Const.K * 17));
    }

    public static Data allocateReadBuffer(){
        return readBuffers.poll();
    }

    public static void recycle(Data Data){
        Data.clear();
        readBuffers.add(Data);
    }

    public static void initBuffers(){
        for (int i = 0; i < 5000; i ++){
            buffers.add(ByteBuffer.allocateDirect((int) (Const.K * 17)));
        }
        for (int i = 0; i < 80000; i ++){
            readBuffers.add(new Dram(ByteBuffer.allocateDirect((int) (Const.K * 17))));
        }
        for (int i = 0; i < 150000; i ++){
            readBuffers.add(new Dram(ByteBuffer.allocate((int) (Const.K * 17))));
        }
    }
}
