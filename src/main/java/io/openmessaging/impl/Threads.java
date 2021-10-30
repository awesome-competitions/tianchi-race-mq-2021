package io.openmessaging.impl;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Threads {

    private static final ThreadLocal<Context> CTX = new ThreadLocal<>();

    private static final AtomicInteger size = new AtomicInteger();

    public static int size(){
        return size.get();
    }

    /**
     * 获取当前线程上下文
     *
     * @return {@link Context}
     */
    public static Context get(){
        Context ctx = CTX.get();
        if (ctx == null){
            ctx = new Context();
            size.incrementAndGet();
            CTX.set(ctx);
        }
        if (! ctx.isPrepare()){
            try {
                // 第一次阻塞50ms用来收集准确的线程总数
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ctx.setPrepare(true);
        }
        return ctx;
    }

    public static class Context{
        private Barrier barrier;
        private boolean prepare;
        public final ThreadPoolExecutor pools = (ThreadPoolExecutor) Executors.newFixedThreadPool(50);
        private final ResultMap results = new ResultMap();
        private final Semaphore semaphore = new Semaphore(0);
        private final ByteBuffer[] buffers = new ByteBuffer[Const.MAX_FETCH_NUM];
        private final LinkedBlockingQueue<ByteBuffer> aepBuffers = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Data> readBuffers2 = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Data> readBuffers3 = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Data> readBuffers4 = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Data> readBuffers5 = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Data> idles1 = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Data> idles2 = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Data> idles3 = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Data> idles4 = new LinkedBlockingQueue<>();

        public Context() {
            for (int i = 0; i < 30; i ++){
                buffers[i] = ByteBuffer.allocateDirect(Const.PROTOCOL_DATA_MAX_SIZE);
            }
        }

        // result buffer 复用
        public ByteBuffer allocateBuffer(int index){
            ByteBuffer buffer = buffers[index];
            if (buffer == null){
                buffer = ByteBuffer.allocateDirect(Const.PROTOCOL_DATA_MAX_SIZE);
                buffers[index] = buffer;
            }
            buffer.clear();
            return buffer;
        }

        public Data allocateReadBuffer(int cap){
            Data data = getReadBufferGreed(cap).poll();
            if (data == null){
                data = getReadBufferGreed((int) (cap + Const.K * 3.4)).poll();
            }
            return data;
        }

        public void recycleReadBuffer(Data data){
            data.clear();
            LinkedBlockingQueue<Data> buffers = getReadBuffer(data.getCapacity());
            if (buffers != null){
                buffers.add(data);
            }else {
                ByteBuffer buffer = ((Dram) data).getData();
                if (buffer instanceof DirectBuffer){
                    Utils.recycleByteBuffer(buffer);
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

        // 17K / 4 4组aep回收池
        public LinkedBlockingQueue<Data> getIdles(int cap){
            return cap < Const.K * 4.5 ? idles1 : cap < Const.K * 9 ? idles2 : cap < Const.K * 13.5 ? idles3 : idles4;
        }

        public LinkedBlockingQueue<Data> getIdlesGreed(int cap){
            return cap < Const.K * 4.5 ? idles2 : cap < Const.K * 9 ? idles3 : idles4;
        }

        // 17K / 5 五组内存回收池
        public LinkedBlockingQueue<Data> getReadBuffer(int cap){
            return cap < Const.K * 3.4 ? null : cap < Const.K * 6.8 ? readBuffers2 : cap < Const.K * 10.2 ? readBuffers3 : cap < Const.K * 13.6 ? readBuffers4: readBuffers5;
        }

        public LinkedBlockingQueue<Data> getReadBufferGreed(int cap){
            return cap < Const.K * 3.4 ? readBuffers2 : cap < Const.K * 6.8 ? readBuffers3 : cap < Const.K * 10.2 ? readBuffers4 : readBuffers5;
        }

        public Barrier getBarrier() {
            return barrier;
        }

        public void setBarrier(Barrier barrier) {
            this.barrier = barrier;
        }

        public Semaphore getSemaphore() {
            return semaphore;
        }

        public ResultMap getResults() {
            return results;
        }

        public ThreadPoolExecutor getPools() {
            return pools;
        }

        public boolean isPrepare() {
            return prepare;
        }

        public void setPrepare(boolean prepare) {
            this.prepare = prepare;
        }

        public LinkedBlockingQueue<ByteBuffer> getAepBuffers() {
            return aepBuffers;
        }
    }
}
