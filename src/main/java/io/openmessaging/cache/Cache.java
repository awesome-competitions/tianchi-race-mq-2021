package io.openmessaging.cache;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.consts.Const;
import io.openmessaging.model.*;
import io.openmessaging.model.Queue;
import io.openmessaging.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
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
        final int lruSizeFinal = lruSize;
        new Thread(()->{
//            int directDramSize = (int) (1.6 * Const.G / pageSize);
//            for (int i = 0; i < directDramSize; i ++){
//                pools.add(applyDram(true));
//            }
//            int heapDramSize = (int) (0.5 * Const.G / pageSize);
//            for (int i = 0; i < heapDramSize; i ++){
//                pools.add(applyDram(false));
//            }
            for (int i = 0; i < lruSizeFinal; i ++){
                pools.add(applyPMem(false));
            }
        }).start();

    }

    public void write(Topic topic, Queue queue, Group group, Segment segment, ByteBuffer byteBuffer) throws InterruptedException {
        Storage storage = queue.getStorage();
        if (storage != null && storage.getIdx() == segment.getIdx()){
            storage.write(byteBuffer);
        }
//        Storage storage = loadStorage(topic, queue, group, segment);
//        storage.write(bytes);
    }

    public Storage loadStorage(Topic topic, Queue queue, Group group, Segment segment) throws InterruptedException {
        lru.computeIfAbsent(topic.getId() + queue.getId(), k -> queue);
        Storage storage = queue.getStorage();
        if (storage == null || storage.getIdx() != segment.getIdx()){
            if (storage == null){
                storage = pools.take();
                queue.setStorage(storage);
            }
            storage.reset(segment.getIdx(), segment.load(group.getDb(), storage.isDirect()), segment.getStart());
        }
        return storage;
    }

    public Storage applyPMem(boolean direct){
        if (heap == null){
            return applyDram(direct);
        }
        return new PMem(heap.allocateMemoryBlock(pageSize));
    }

    public Storage applyDram(boolean direct){
        return new Dram(direct);
    }
}
