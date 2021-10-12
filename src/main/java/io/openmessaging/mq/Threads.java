package io.openmessaging.mq;

import io.openmessaging.consts.Const;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

public class Threads {

    private static final ThreadLocal<Context> CTX = new ThreadLocal<>();

    public static Context get(){
        Context ctx = CTX.get();
        if (ctx == null){
            ctx = new Context();
            CTX.set(ctx);
        }
        return ctx;
    }

    public static class Context{

        private ByteBuffer buffer;

        private Barrier barrier;

        private int blockPos;

        private long ssdPos;

        private final LinkedList<ByteBuffer> buffers;

        private final LinkedList<Data> readBuffers;

        private final LinkedList<Data> idles1 = new LinkedList<>();
        private final LinkedList<Data> idles2 = new LinkedList<>();
        private final LinkedList<Data> idles3 = new LinkedList<>();
        private final LinkedList<Data> idles4 = new LinkedList<>();

        public ByteBuffer allocateBuffer(){
            ByteBuffer buffer = buffers.poll();
            if (buffer == null){
                buffer = Buffers.allocateExtraBuffer();
            }
            buffer.clear();
            buffers.add(buffer);
            return buffer;
        }

        public Data allocateReadBuffer(){
            Data data = readBuffers.poll();
            if (data != null){
                Monitor.writeDramCount ++;
            }
            return data;
        }

        public void recycleReadBuffer(Data data){
            data.clear();
            readBuffers.add(data);
        }

        public void recyclePMem(Data data){
            data.clear();
            getIdles(data.getCapacity()).add(data);
        }

        public LinkedList<Data> getIdles(int cap){
            if (cap < Const.K * 4.5){
                return idles1;
            }else if (cap < Const.K * 9){
                return idles2;
            }else if (cap < Const.K * 13.5){
                return idles3;
            }else{
                return idles4;
            }
        }

        public Context() {
            this.buffer = ByteBuffer.allocateDirect((int) (Const.K * 17) + 9);
            this.buffers = new LinkedList<>();
            this.readBuffers = new LinkedList<>();
            for (int i = 0; i < 400; i ++){
                buffers.add(ByteBuffer.allocateDirect((int) (Const.K * 17)));
            }
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
