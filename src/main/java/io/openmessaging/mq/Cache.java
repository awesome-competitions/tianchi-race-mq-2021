package io.openmessaging.mq;

import com.intel.pmem.llpl.Heap;

public class Cache {

    private Heap heap;

    private long used;

    private final Config config;

    public Cache(Config config){
        if (config.getHeapDir() != null){
            this.heap = Heap.exists(config.getHeapDir()) ? Heap.openHeap(config.getHeapDir()) : Heap.createHeap(config.getHeapDir(), config.getHeapSize());
        }
        this.config = config;
    }

    public synchronized Data apply(int capacity){
        if (heap == null){
            return new Dram(capacity);
        }
        if (config.getHeapUsableSize() - used > capacity){
            used += capacity;
            Monitor.heapUsedSize = used;
            return new PMem(heap.allocateCompactMemoryBlock(capacity), capacity);
        }
        return null;
    }

    public synchronized void recycle(Data data){
        if (heap != null){
            data.clear();
            used -= data.getCapacity();
            Monitor.heapUsedSize = used;
        }
    }

}
