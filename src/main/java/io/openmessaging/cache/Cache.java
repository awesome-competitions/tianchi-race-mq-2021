package io.openmessaging.cache;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import io.openmessaging.model.*;
import io.openmessaging.model.Readable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

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
            PMemBlock storage = queue.getStorage();
            if (storage != null && storage.getIdx() == segment.getIdx()){
                storage.write(bytes);
            }
        }finally {
            queue.getLock().readLock().unlock();
        }
    }

    public Storage loadStorage(Queue queue, Group group, Segment segment){
        PMemBlock storage = queue.getStorage();
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

    private Storage loadDram(Group group, Segment segment){
        return new Dram(segment.load(group.getDb()), segment.getStart());
    }

    private Storage loadPMem(Group group, Segment segment){
        if (segment.getAos() == segment.getPos()){
            return null;
        }
        List<ByteBuffer> buffers = segment.load(group.getDb());
        List<AnyMemoryBlock> blocks = new ArrayList<>(buffers.size());
        for (ByteBuffer buffer: buffers){
            AnyMemoryBlock anyMemoryBlock = heap.allocateMemoryBlock(buffer.capacity());
            anyMemoryBlock.copyFromArray(buffer.array(), 0, 0, buffer.capacity());
            blocks.add(anyMemoryBlock);
        }
        return new PMem(heap, blocks, segment.getStart());
    }

    private Storage loadPMemBlock(Group group, Segment segment){
        if (segment.getAos() == segment.getPos()){
            return null;
        }
        return new PMemBlock(heap.allocateMemoryBlock(pageSize), segment.load(group.getDb()), segment.getStart());
    }

    public PMemBlock applyBlock(){
        return new PMemBlock(heap.allocateMemoryBlock(pageSize));
    }
}
