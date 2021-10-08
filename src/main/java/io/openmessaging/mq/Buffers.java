package io.openmessaging.mq;

import io.openmessaging.consts.Const;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

public class Buffers {

    private static final LinkedBlockingQueue<ByteBuffer> buffers = new LinkedBlockingQueue<>();

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

    public static void initBuffers(){
        for (int i = 0; i < 20000; i ++){
            buffers.add(ByteBuffer.allocate((int) (Const.K * 17)));
        }
    }
}
