package io.openmessaging.cache;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
import io.openmessaging.consts.Const;
import io.openmessaging.model.*;
import io.openmessaging.model.Queue;
import io.openmessaging.model.Readable;
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

    private final Lru<Segment> lru;

    private final long pageSize;

    private final LinkedBlockingQueue<Storage> pools;

    private Group group;

    private volatile boolean ready;

    private final static Logger LOGGER = LoggerFactory.getLogger(Cache.class);

    private int lruToSDDTimes;

    private int lruReadFromSSDTimes;

    private int lruWriteFromSSDTimes;

    private int lruWeedOutTimes;

    public Cache(String path, long heapSize, int lruSize, long pageSize, Group group){
        if (Objects.nonNull(path)){
            this.heap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
        }
        if (lruSize < 1000){
            lruSize = 1000;
        }
        this.pageSize = pageSize;
        this.group = group;
        this.pools = new LinkedBlockingQueue<>();
        this.lru = new Lru<>(lruSize - 500, k -> {
            try{
                lruWeedOutTimes ++;
                k.lock();
                Storage storage = k.getStorage();
                if (storage != null && ! (storage instanceof SSD)){
                    if (k.getQueue().getHead().getIdx() == k.getIdx() || k.getEnd() >= k.getQueue().getReadOffset()){
                        Storage ssd = new SSD(group.getAndIncrementOffset() * pageSize, pageSize, group.getDb());
                        ssd.reset(0, storage.load(), k.getStart());
                        k.setStorage(ssd);
                        lruToSDDTimes ++;
                    }else{
                        k.setStorage(null);
                    }
                    pools.add(storage);
                }
            }finally {
                k.unlock();
            }
        });
        int finalLruSize = lruSize;
        Thread thread = new Thread(()->{
            for (int i = 0; i < finalLruSize; i ++){
                pools.add(applyPMem(false));
            }
            ready = true;
            LOGGER.info("pmem is ready");
            while (true){
                try {
                    Thread.sleep(30 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                LOGGER.info("lruWeedOutTimes {}, lruToSDDTimes {}, lruReadFromSSDTimes {}, lruWriteFromSSDTimes {}", lruWeedOutTimes, lruToSDDTimes, lruReadFromSSDTimes, lruWriteFromSSDTimes);
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    public Segment applySegment(Topic topic, Queue queue, long offset) throws InterruptedException {
        Segment segment = new Segment(queue, topic.getId(), queue.getId(), offset, offset, pageSize);
        queue.addSegment(segment);
        Storage storage = pools.take();
        storage.reset(segment.getIdx(), new ArrayList<>(), offset);
        segment.setStorage(storage);
        return lru.add(segment);
    }

    public void write(Segment segment, ByteBuffer byteBuffer){
        try{
            segment.lock();
            if (segment.getStorage() instanceof SSD){
                Storage other = pools.take();
                other.reset(segment.getIdx(), segment.getStorage().load(), segment.getStart());
                segment.setStorage(other);
                lruWeedOutTimes ++;
            }
            segment.write(byteBuffer);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            segment.unlock();
            lru.add(segment);
        }
    }

    public List<ByteBuffer> read(Readable readable) throws InterruptedException {
        Segment segment = readable.getSegment();
        try{
            segment.lock();
            if (segment.getStorage() instanceof SSD){
                Storage other = pools.take();
                other.reset(segment.getIdx(), segment.getStorage().load(), segment.getStart());
                segment.setStorage(other);
                lruReadFromSSDTimes ++;
            }
            return readable.getSegment().read(readable.getStartOffset(), readable.getEndOffset());
        }finally {
            segment.unlock();
            lru.add(segment);
        }
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

//    public void clearSegments(List<Segment> segments, Segment last){
//        for (int i = 0; i < last.getIdx(); i ++){
//            clearSegment(segments.get(i));
//        }
//    }

//    public void clearSegment(Segment segment){
//        Storage storage = segment.getStorage();
//        if (! (storage instanceof SSD)){
//            lru.remove(segment);
//            pools.add(storage);
//        }
//    }
}
