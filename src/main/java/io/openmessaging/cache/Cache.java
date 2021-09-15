package io.openmessaging.cache;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class Cache {

    private Heap heap;

    private final static Logger LOGGER = LoggerFactory.getLogger(Cache.class);

    private final long pageSize;

    public Cache(String path, long heapSize, long pageSize){
        if (Objects.nonNull(path)){
            this.heap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
        }
        this.pageSize = pageSize;
    }

    public void write(Queue queue, Segment segment, byte[] bytes){
        try{
            queue.getLock().readLock().lock();
            Storage storage = queue.getStorage();
            if (storage != null && storage.getIdx() == segment.getIdx()){
                storage.write(bytes);
            }
        }finally {
            queue.getLock().readLock().unlock();
        }
    }

    public Storage loadStorage(Queue queue, Group group, Segment segment){
        Storage storage = queue.getStorage();
        if (storage.getIdx() != segment.getIdx()){
            try{
                queue.getLock().writeLock().lock();
                storage = queue.getStorage();
                if (storage.getIdx() != segment.getIdx()){
                    storage.reset(segment.getIdx(), segment.load(group.getDb()), segment.getStart());
                }
            }finally {
                queue.getLock().writeLock().unlock();
            }
        }
        return storage;
    }

    public Storage applyBlock(){
        if (heap == null){
            return new Dram();
        }
        return new PMem(heap.allocateMemoryBlock(pageSize));
    }
}
