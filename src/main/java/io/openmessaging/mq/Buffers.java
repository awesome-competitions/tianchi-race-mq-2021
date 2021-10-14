package io.openmessaging.mq;

import io.openmessaging.consts.Const;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

public class Buffers {

    public static final LinkedBlockingQueue<ByteBuffer> AEP_BUFFERS = new LinkedBlockingQueue<>();

    public static Data allocateReadBuffer(){
        if (Monitor.writeDramCount < 80000){
            Monitor.writeDramCount ++;
            return new Dram(ByteBuffer.allocateDirect((int) (Const.K * 17)));
        }else if (Monitor.writeDramCount < 250000){
            Monitor.writeDramCount ++;
            return new Dram(ByteBuffer.allocate((int) (Const.K * 17)));
        }
        return null;
    }

    public static Data allocateExtraData(){
        return new Dram(allocateExtraBuffer());
    }

    public static ByteBuffer allocateExtraBuffer(){
        Monitor.writeExtraDramCount ++;
        return ByteBuffer.allocate((int) (Const.K * 17));
    }
}
