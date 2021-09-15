package io.openmessaging.cache;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.model.*;
import io.openmessaging.model.Queue;
import io.openmessaging.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class Cache {

    private Heap heap;

    private final static Logger LOGGER = LoggerFactory.getLogger(Cache.class);

    private final Lru<Integer, Queue> lru;

    private final List<Storage> cleans;

    private final long pageSize;

    public Cache(String path, long heapSize, int lruSize, long pageSize){
        if (Objects.nonNull(path)){
            this.heap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
        }
        this.pageSize = pageSize;
        this.cleans = new CopyOnWriteArrayList<>();
        this.lru = new Lru<>(lruSize, v -> {
            Storage storage = v.getStorage();
            if (storage != null){
                v.setStorage(null);
                storage.killed();
                cleans.add(storage);
            }
        });
        Thread cleanJob = new Thread(this::clean);
        cleanJob.setDaemon(true);
        cleanJob.start();
    }

    public void clean(){
        while (true){
            try {
                Thread.sleep(1000);
                long curr = System.currentTimeMillis();
                if (CollectionUtils.isNotEmpty(cleans)){
                    Iterator<Storage> it = cleans.iterator();
                    while(it.hasNext()){
                        Storage s = it.next();
                        if (curr > s.expire()){
                            s.clean();
                        }
                        cleans.remove(s);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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

    public Storage loadStorage(Queue queue, Group group, Segment segment){
        lru.computeIfAbsent(queue.getId(), k -> queue);
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
                        queue.setStorage(storage = applyBlock());
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
