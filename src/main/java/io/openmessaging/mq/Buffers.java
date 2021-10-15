package io.openmessaging.mq;

import io.openmessaging.consts.Const;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Buffers {

    public static final LinkedBlockingQueue<ByteBuffer> AEP_BUFFERS = new LinkedBlockingQueue<>();

    public static final long DIRECT_SIZE = (long) (Const.G * 1.65);
    public static final long HEAP_SIZE = (long) (Const.G * 2.85);
    public static final long MAX_SIZE = DIRECT_SIZE + HEAP_SIZE;
    public static final long THRESHOLD_SIZE = MAX_SIZE - Const.M;

    public static Data allocateReadBuffer(int cap){
        if (Monitor.writeDramSize < DIRECT_SIZE){
            Monitor.writeDramCount ++;
            Monitor.writeDramSize += cap;
            return new Dram(ByteBuffer.allocateDirect(cap));
        }else if (Monitor.writeDramSize < MAX_SIZE){
            Monitor.writeDramCount ++;
            Monitor.writeDramSize += cap;
            return new Dram(ByteBuffer.allocate(cap));
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
