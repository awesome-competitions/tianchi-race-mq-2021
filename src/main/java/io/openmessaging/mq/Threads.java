package io.openmessaging.mq;

import io.openmessaging.consts.Const;

import java.nio.ByteBuffer;

public class Threads {

    private static final Context[] CTX = new Context[100];

    public static Context get(){
        int tid = (int) Thread.currentThread().getId();
        Context ctx = CTX[tid];
        if (ctx == null){
            ctx = new Context();
            CTX[tid] = ctx;
        }
        return ctx;
    }

    public static class Context{

        private ByteBuffer buffer;

        private Barrier barrier;

        private int blockPos;

        private long ssdPos;

        public Context() {
            this.buffer = ByteBuffer.allocateDirect((int) (Const.K * 17) + 9);
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        public Barrier getBarrier() {
            return barrier;
        }

        public void setBarrier(Barrier barrier) {
            this.barrier = barrier;
        }

        public int getBlockPos() {
            return blockPos;
        }

        public void blockPosIncrement(){
            ++blockPos;
        }

        public void setBlockPos(int blockPos) {
            this.blockPos = blockPos;
        }

        public long getSsdPos() {
            return ssdPos;
        }

        public void setSsdPos(long ssdPos) {
            this.ssdPos = ssdPos;
        }
    }
}
