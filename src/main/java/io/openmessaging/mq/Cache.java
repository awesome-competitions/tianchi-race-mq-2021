package io.openmessaging.mq;

import com.intel.pmem.llpl.Heap;

import java.util.concurrent.atomic.AtomicLong;

public class Cache {

    private Heap heap;

    private final AtomicLong used;

    private final Config config;

    public Cache(Config config){
        if (config.getHeapDir() != null){
            this.heap = Heap.exists(config.getHeapDir()) ? Heap.openHeap(config.getHeapDir()) : Heap.createHeap(config.getHeapDir(), config.getHeapSize());
        }
        this.config = config;
        this.used = new AtomicLong();
    }

    public Data apply(int capacity){
        if (heap == null){
            return new Dram(capacity);
        }
        if (used.addAndGet(capacity) < config.getHeapUsableSize()){
            Monitor.heapUsedSize = used.longValue();
            return new PMem(heap.allocateCompactMemoryBlock(capacity), capacity);
        }else{
            used.addAndGet(- capacity);
        }
        return null;
    }

    public void recycle(Data data){
        if (heap != null && data instanceof PMem){
            data.clear();
            used.addAndGet(- data.getCapacity());
            Monitor.heapUsedSize = used.longValue();
        }
    }

}
