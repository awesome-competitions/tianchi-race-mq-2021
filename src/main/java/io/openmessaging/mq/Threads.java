package io.openmessaging.mq;

import io.openmessaging.consts.Const;
import io.openmessaging.utils.BufferUtils;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.*;
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

        private Barrier barrier;

        private int blockPos;

        private long ssdPos;

        public final ThreadPoolExecutor pools = (ThreadPoolExecutor) Executors.newFixedThreadPool(30);

        private final Map<Integer, ByteBuffer> results = new ArrayMap();

        private final Semaphore semaphore = new Semaphore(0);

        private final List<MappedByteBuffer> mappedByteBuffers = new ArrayList<>();
        
        private final LinkedList<ByteBuffer> buffers = new LinkedList<>();

        private final LinkedList<Data> readBuffers1 = new LinkedList<>();
        private final LinkedList<Data> readBuffers2 = new LinkedList<>();
        private final LinkedList<Data> readBuffers3 = new LinkedList<>();
        private final LinkedList<Data> readBuffers4 = new LinkedList<>();
        private final LinkedList<Data> readBuffers5 = new LinkedList<>();

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

        public Data allocateReadBuffer(int cap){
            Data data = getReadBufferGreed(cap).poll();
            if (data == null){
                data = getReadBufferGreed((int) (cap + Const.K * 3.4)).poll();
            }
            if (data != null){
                Monitor.writeDramCount ++;
            }
            return data;
        }

        public void recycleReadBuffer(Data data){
            data.clear();
            LinkedList<Data> buffers = getReadBuffer(data.getCapacity());
            if (buffers != null){
                buffers.add(data);
            }else {
                ByteBuffer buffer = ((Dram) data).getData();
                if (buffer instanceof DirectBuffer){
                    BufferUtils.clean(buffer);
                }
            }
        }

        public Data allocatePMem(int cap){
            return getIdlesGreed(cap).poll();
        }

        public void recyclePMem(Data data){
            data.clear();
            getIdles(data.getCapacity()).add(data);
        }

        public LinkedList<Data> getIdles(int cap){
            return cap < Const.K * 4.5 ? idles1 : cap < Const.K * 9 ? idles2 : cap < Const.K * 13.5 ? idles3 : idles4;
        }

        public LinkedList<Data> getIdlesGreed(int cap){
            return cap < Const.K * 4.5 ? idles2 : cap < Const.K * 9 ? idles3 : idles4;
        }

        public LinkedList<Data> getReadBuffer(int cap){
            return cap < Const.K * 3.4 ? null : cap < Const.K * 6.8 ? readBuffers2 : cap < Const.K * 10.2 ? readBuffers3 : cap < Const.K * 13.6 ? readBuffers4: readBuffers5;
        }

        public LinkedList<Data> getReadBufferGreed(int cap){
            return cap < Const.K * 3.4 ? readBuffers2 : cap < Const.K * 6.8 ? readBuffers3 : cap < Const.K * 10.2 ? readBuffers4 : readBuffers5;
        }

        public Context() {
            for (int i = 0; i < 50; i ++){
                buffers.add(ByteBuffer.allocateDirect((int) (Const.K * 17)));
            }
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

        public List<MappedByteBuffer> getMappedByteBuffers() {
            return mappedByteBuffers;
        }
    }
}
