package io.openmessaging.cache;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
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

    public Cache(String path, long heapSize){
        if (Objects.nonNull(path)){
            this.heap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
        }
    }

    public void write(Segment segment, byte[] bytes){
        try{
            segment.readLock().lock();
            Storage storage = segment.getStorage();
            if (storage != null){
                storage.write(bytes);
            }
        }finally {
            segment.readLock().unlock();
        }
    }

    public Storage loadStorage(Group group, Segment segment){
        Storage storage = segment.getStorage();
        if (storage == null){
            try {
                segment.writeLock().lock();
                if (segment.getStorage() == null){
                    storage = heap == null ? loadDram(group, segment) : loadPMem(group, segment);
                }
                segment.setStorage(storage);
            }finally {
                segment.writeLock().unlock();
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
}
