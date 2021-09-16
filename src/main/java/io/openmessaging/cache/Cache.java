package io.openmessaging.cache;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.model.*;
import io.openmessaging.model.Queue;
import io.openmessaging.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class Cache {

    private Heap heap;

    private final static Logger LOGGER = LoggerFactory.getLogger(Cache.class);

    private final Lru<Integer, Queue> lru;

    private final long pageSize;

    private final LinkedBlockingQueue<Storage> pools;

    public Cache(String path, long heapSize, int lruSize, long pageSize){
        if (Objects.nonNull(path)){
            this.heap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
        }
        if (lruSize < 1000){
            lruSize = 1000;
        }
        this.pageSize = pageSize;
        this.pools = new LinkedBlockingQueue<>();
        this.lru = new Lru<>(lruSize - 500, v -> {
            Storage storage = v.getStorage();
            if (storage != null){
                v.setStorage(null);
                pools.add(storage);
            }
        });
        for (int i = 0; i < lruSize; i ++){
            pools.add(applyBlock());
        }
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

    public Storage loadStorage(Topic topic, Queue queue, Group group, Segment segment) throws InterruptedException {
        lru.computeIfAbsent(topic.getId() + queue.getId(), k -> queue);
        Storage storage = queue.getStorage();
        if (storage == null || storage.getIdx() != segment.getIdx()){
            boolean locked = queue.getHead().getIdx() == segment.getIdx();
            try{
                if (locked){
                    queue.getLock().writeLock().lock();
                }
                storage = queue.getStorage();
                if (storage == null || storage.getIdx() != segment.getIdx()){
                    if (storage == null){
                        storage = pools.take();
                        queue.setStorage(storage);
                    }
                    storage.reset(segment.getIdx(), segment.load(group.getDb()), segment.getStart());
                }
            }finally {
                if (locked){
                    queue.getLock().writeLock().unlock();
                }
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
