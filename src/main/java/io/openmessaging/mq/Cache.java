package io.openmessaging.mq;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.consts.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class Cache {

    private Heap heap;

    private final List<Block> blocks = new ArrayList<>(10);

    private final LinkedBlockingQueue<Data> idles1 = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Data> idles2 = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Data> idles3 = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Data> idles4 = new LinkedBlockingQueue<>();

    private static final long BLOCK_SIZE = Const.G * 5;

    private static final Logger LOGGER = LoggerFactory.getLogger(Cache.class);

    public Cache(String heapDir, long heapSize){
        if (heapDir != null){
            this.heap = Heap.exists(heapDir) ? Heap.openHeap(heapDir) : Heap.createHeap(heapDir, heapSize);
            this.blocks.add(applyBlock(Const.G * 5));
            startProducer();
        }
    }

    private void startProducer(){
        Thread producer = new Thread(() -> {
            for (int i = 0; i < 10; i ++){
                this.blocks.add(applyBlock(BLOCK_SIZE));
            }
        });
        producer.setDaemon(true);
        producer.start();
    }

    public Block applyBlock(long size){
        return new Block(heap.allocateCompactMemoryBlock(size), size);
    }

    public Block localBlock(){
        if (Threads.get().getBlockPos() < blocks.size()){
            return blocks.get(Threads.get().getBlockPos());
        }
        return null;
    }

    public Data allocate(int cap){
        if (heap == null){
            return new Dram(cap);
        }
        long memPos = -1;
        while (Threads.get().getBlockPos() < blocks.size() && (memPos = localBlock().allocate(cap)) == -1){
            Threads.get().blockPosIncrement();
        }
        if (memPos == -1){
            Data data = getIdles(cap).poll();
            if (data == null){
                Monitor.missingIdleCount ++;
            }else{
                Monitor.allocateIdleCount ++;
            }
            return data;
        }
        return new PMem(localBlock(), memPos, cap);
    }

    public Data take(int cap){
        try {
            return getIdles(cap).take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void recycle(Data data){
        if (data instanceof PMem){
            data.clear();
            getIdles(data.getCapacity()).add(data);
        }
    }

    private LinkedBlockingQueue<Data> getIdles(int cap){
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

}
