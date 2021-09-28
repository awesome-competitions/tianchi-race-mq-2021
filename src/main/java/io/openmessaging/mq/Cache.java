package io.openmessaging.mq;

import com.intel.pmem.llpl.Heap;

import java.util.concurrent.atomic.AtomicLong;

public class Cache {

    private Heap heap;

    public Cache(Config config){
        if (config.getHeapDir() != null){
            this.heap = Heap.exists(config.getHeapDir()) ? Heap.openHeap(config.getHeapDir()) : Heap.createHeap(config.getHeapDir(), config.getHeapSize());
        }
    }

    public Data apply(int capacity){
        if (heap == null){
            return new Dram(capacity);
        }
        Monitor.heapUsedSize += capacity;
        return new PMem(heap.allocateCompactMemoryBlock(capacity), capacity);
    }

}
