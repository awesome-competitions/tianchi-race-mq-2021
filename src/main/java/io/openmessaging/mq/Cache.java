package io.openmessaging.mq;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.consts.Const;
import io.openmessaging.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Cache {

    private Heap heap;

    private final List<Block> blocks = new ArrayList<>(10);

    private final LinkedBlockingQueue<Data> idles = new LinkedBlockingQueue<>();

    private final ThreadLocal<Integer> blockPos = new ThreadLocal<>();

    private static final long BLOCK_SIZE = Const.G * 5;

    private static final Logger LOGGER = LoggerFactory.getLogger(Cache.class);

    public Cache(String heapDir, long heapSize){
        if (heapDir != null){
            this.heap = Heap.exists(heapDir) ? Heap.openHeap(heapDir) : Heap.createHeap(heapDir, heapSize);
            this.blocks.add(applyBlock(Const.G * 5));
            startProducer();
        }
        Buffers.initBuffers();
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
        if (blockPos.get() < blocks.size()){
            return blocks.get(blockPos.get());
        }
        return null;
    }

    public Data allocate(int cap){
        if (heap == null){
            return new Dram(cap);
        }
        if (blockPos.get() == null){
            blockPos.set(0);
        }
        long memPos = -1;
        while (blockPos.get() < blocks.size() && (memPos = localBlock().allocate(cap)) == -1){
            blockPos.set(blockPos.get() + 1);
        }
        if (memPos == -1){
            Data data = idles.poll();
            if (data == null){
                Monitor.missingIdleCount ++;
            }else{
                Monitor.allocateIdleCount ++;
            }
            return data;
        }
        return new PMem(localBlock(), memPos, cap);
    }

    public void recycle(Data data){
        if (data instanceof PMem){
            data.clear();
            idles.add(data);
        }
    }

}
