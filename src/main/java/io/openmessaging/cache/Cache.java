package io.openmessaging.cache;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class Cache {

    private Heap heap;

    private final static Logger LOGGER = LoggerFactory.getLogger(Cache.class);

    private final Lru<Integer, Queue> lru;

    private final long pageSize;

    public void addLru(Queue queue){
        lru.put(queue.getId(), queue);
    }

    public Cache(String path, long heapSize, int lruSize, long pageSize){
        if (Objects.nonNull(path)){
            this.heap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
        }
        this.pageSize = pageSize;
        this.lru = new Lru<>(lruSize, v -> {
            try{
                System.out.println("淘汰" + v.getId());
                v.getLock().writeLock().lock();
                Storage storage = v.getStorage();
                if (storage != null){
                    v.setStorage(null);
                    storage.clean();
                }
            }finally {
                v.getLock().writeLock().unlock();
                System.out.println("un淘汰" + v.getId());
            }
        });
    }

    public void write(Queue queue, Segment segment, byte[] bytes){
        try{
            queue.getLock().readLock().lock();
            Storage storage = queue.getStorage();
            if (storage != null && storage.getIdx() == segment.getIdx()){
                lru.computeIfAbsent(queue.getId(), k -> queue);
                storage.write(bytes);
            }
        }finally {
            queue.getLock().readLock().unlock();
        }
    }

    public Storage loadStorage(Queue queue, Group group, Segment segment){
        Storage storage = queue.getStorage();
        if (storage == null || storage.getIdx() != segment.getIdx()){
            try{
                System.out.println("load" + queue.getId());
                queue.getLock().writeLock().lock();
                storage = queue.getStorage();
                if (storage == null || storage.getIdx() != segment.getIdx()){
                    if (storage == null){
                        queue.setStorage(storage = applyBlock());
                    }
                    lru.computeIfAbsent(queue.getId(), k -> queue);
                    storage.reset(segment.getIdx(), segment.load(group.getDb()), segment.getStart());
                }
            }finally {
                queue.getLock().writeLock().unlock();
                System.out.println("unload" + queue.getId());
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
