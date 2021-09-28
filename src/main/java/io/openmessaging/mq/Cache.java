package io.openmessaging.mq;

import com.intel.pmem.llpl.Heap;

public class Cache {

    private Heap heap;

    private long used;

    private long size;

    private Config config;

    public Cache(Config config){
        if (config.getHeapDir() != null){
            this.heap = Heap.exists(config.getHeapDir()) ? Heap.openHeap(config.getHeapDir()) : Heap.createHeap(config.getHeapDir(), config.getHeapSize());
        }
    }

    public Data apply(int capacity){
        if (heap == null){
            return new Dram(capacity);
        }
        if (size - used > )

        return new PMem(heap.allocateCompactMemoryBlock(capacity), capacity);
    }



}
