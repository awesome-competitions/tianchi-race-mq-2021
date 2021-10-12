package io.openmessaging.mq;

import io.openmessaging.consts.Const;

import java.nio.ByteBuffer;

public class Buffers {

    public static Data allocateReadBuffer(){
        if (Monitor.writeDramCount < 50000){
            return new Dram(ByteBuffer.allocateDirect((int) (Const.K * 17)));
        }else if (Monitor.writeDramCount < 120000){
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
