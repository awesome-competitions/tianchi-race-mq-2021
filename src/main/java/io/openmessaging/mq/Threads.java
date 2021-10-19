package io.openmessaging.mq;

import io.openmessaging.consts.Const;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.*;

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

        public final ThreadPoolExecutor pools = (ThreadPoolExecutor) Executors.newFixedThreadPool(30);

        private final Map<Integer, ByteBuffer> results = new ArrayMap();

        private final Semaphore semaphore = new Semaphore(0);

        private final LinkedBlockingQueue<ByteBuffer> buffers = new LinkedBlockingQueue<>();

        private final LinkedBlockingQueue<Data> readBuffers1 = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Data> readBuffers2 = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Data> readBuffers3 = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Data> readBuffers4 = new LinkedBlockingQueue<>();

        private final LinkedBlockingQueue<Data> idles1 = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Data> idles2 = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Data> idles3 = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Data> idles4 = new LinkedBlockingQueue<>();

        public ByteBuffer allocateBuffer(){
            ByteBuffer buffer = buffers.poll();
            if (buffer == null){
                buffer = Buffers.allocateExtraBuffer();
            }
            buffer.clear();
            buffers.add(buffer);
            return buffer;
        }

        public Data allocateReadBuffer(int cap){
            Data data = getReadBufferGreed(cap).poll();
            if (data != null){
                Monitor.writeDramCount ++;
            }
            return data;
        }

        public void recycleReadBuffer(Data data){
            data.clear();
            getReadBuffer(data.getCapacity()).add(data);
        }

        public Data allocatePMem(int cap){
            return getIdlesGreed(cap).poll();
        }

        public void recyclePMem(Data data){
            data.clear();
            getIdles(data.getCapacity()).add(data);
        }

        public LinkedBlockingQueue<Data> getIdles(int cap){
            return cap < Const.K * 4.5 ? idles1 : cap < Const.K * 9 ? idles2 : cap < Const.K * 13.5 ? idles3 : idles4;
        }

        public LinkedBlockingQueue<Data> getIdlesGreed(int cap){
            return cap < Const.K * 4.5 ? idles2 : cap < Const.K * 9 ? idles3 : idles4;
        }

        public LinkedBlockingQueue<Data> getReadBuffer(int cap){
            return cap < Const.K * 4.5 ? readBuffers1 : cap < Const.K * 9 ? readBuffers2 : cap < Const.K * 13.5 ? readBuffers3 : readBuffers4;
        }

        public LinkedBlockingQueue<Data> getReadBufferGreed(int cap){
            return cap < Const.K * 4.5 ? readBuffers2 : cap < Const.K * 9 ? readBuffers3 : readBuffers4;
        }

        public Context() {
            this.buffer = ByteBuffer.allocateDirect((int) (Const.K * 17) + 9);
            for (int i = 0; i < 105; i ++){
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

        public Semaphore getSemaphore() {
            return semaphore;
        }

        public Map<Integer, ByteBuffer> getResults() {
            return results;
        }

        public ThreadPoolExecutor getPools() {
            return pools;
        }
    }
}
