package io.openmessaging.mq;

import io.openmessaging.consts.Const;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Buffers {

    public static final LinkedBlockingQueue<ByteBuffer> AEP_BUFFERS = new LinkedBlockingQueue<>();

    public static final int MAX_SIZE = 100000;
    public static final int THRESHOLD_SIZE = MAX_SIZE - 1000;

    public static Data allocateReadBuffer(){
        if (Monitor.writeDramCount < MAX_SIZE){
            Monitor.writeDramCount ++;
            return new Dram(ByteBuffer.allocateDirect((int) (Const.K * 17)));
        }
//        else if (Monitor.writeDramCount < MAX_SIZE){
//            Monitor.writeDramCount ++;
//            return new Dram(ByteBuffer.allocate((int) (Const.K * 17)));
//        }
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
