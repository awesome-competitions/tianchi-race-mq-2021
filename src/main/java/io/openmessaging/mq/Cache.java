package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
import io.openmessaging.consts.Const;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class Cache {

    private Heap heap;

    private Config config;

    private ThreadLocal<AnyMemoryBlock> blocks;

    public Cache(Config config){
        if (config.getHeapDir() != null){
            this.heap = Heap.exists(config.getHeapDir()) ? Heap.openHeap(config.getHeapDir()) : Heap.createHeap(config.getHeapDir(), config.getHeapSize());
        }
        this.blocks = new ThreadLocal<>();
        this.config = config;
    }

    public AnyMemoryBlock getBlock(){
        return heap.allocateCompactMemoryBlock(Const.G * 5);
    }

    public Data apply(int capacity){
        if (heap == null){
            return new Dram(capacity);
        }
        Monitor.heapUsedSize += capacity;
        return new PMem(heap.allocateCompactMemoryBlock(capacity), capacity);
    }

}
