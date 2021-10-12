package io.openmessaging.mq;

import io.openmessaging.consts.Const;

import java.nio.ByteBuffer;
import java.util.LinkedList;

public class Buffers {

    private static final LinkedList<ByteBuffer> buffers = new LinkedList<>();

    private static final LinkedList<Data> readBuffers = new LinkedList<>();

    static {
        initBuffers();
    }

    public static ByteBuffer allocateBuffer(){
        ByteBuffer buffer = buffers.poll();
        if (buffer == null){
            buffer = allocateExtraBuffer();
        }
        buffer.clear();
        buffers.add(buffer);
        return buffer;
    }

    public static Data allocateReadBuffer(){
        return readBuffers.poll();
    }

    public static void recycle(Data data){
        data.clear();
        readBuffers.add(data);
    }

    public static void initBuffers(){
        for (int i = 0; i < 5000; i ++){
            buffers.add(ByteBuffer.allocateDirect((int) (Const.K * 17)));
        }
        for (int i = 0; i < 70000; i ++){
            readBuffers.add(new Dram(ByteBuffer.allocateDirect((int) (Const.K * 17))));
        }
        for (int i = 0; i < 80000; i ++){
            readBuffers.add(new Dram(ByteBuffer.allocate((int) (Const.K * 17))));
        }
    }

    public static Data allocateExtraData(){
        return new Dram(allocateExtraBuffer());
    }

    public static ByteBuffer allocateExtraBuffer(){
        Monitor.writeExtraDramCount ++;
        return ByteBuffer.allocate((int) (Const.K * 17));
    }
}
