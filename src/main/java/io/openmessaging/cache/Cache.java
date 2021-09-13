package io.openmessaging.cache;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
import io.openmessaging.model.*;
import io.openmessaging.model.Readable;
import io.openmessaging.utils.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Cache {

    private Heap heap;

    private final int blockSize;

    private Lru<Triple<String, Integer, Integer>, AbstractMedium> pMem;

    private Lru<Triple<String, Integer, Integer>, AbstractMedium> dram;

    private final static Logger LOGGER = LoggerFactory.getLogger(Cache.class);

    public Cache(String path, long heapSize, int cacheSize, int blockSize){
        if (Objects.nonNull(path)){
            this.heap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
            this.pMem = new Lru<>(cacheSize, AbstractMedium::clean);
        }else{
            this.dram = new Lru<>(cacheSize, AbstractMedium::clean);
        }
        this.blockSize = blockSize;
    }

    public List<ByteBuffer> load(Topic topic, Queue queue, long offset, int num){
        Segment segment = queue.getSegment(offset);
        if (segment == null){
            return null;
        }
        Group group = topic.getGroup(queue.getId());
        long startOffset = offset;
        long endOffset = offset + num - 1;
        List<Readable> readableList = new ArrayList<>();
        while (num > 0 && segment != null){
            if (segment.getEnd() >= endOffset){
                readableList.add(new Readable(segment, startOffset, endOffset));
                break;
            }else{
                readableList.add(new Readable(segment, startOffset, segment.getEnd()));
                long score = segment.getEnd() - startOffset;
                num -= score + 1;
                startOffset += score;
                segment = queue.nextSegment(segment);
            }
        }
        List<ByteBuffer> buffers = new ArrayList<>(num);
        for (Readable readable: readableList){
            AbstractMedium mm = loadMedium(topic, queue, group, readable.getSegment());
            buffers.addAll(mm.read(readable.getStartOffset(), readable.getEndOffset()));
        }
        return buffers;
    }

    public void write(Topic topic, Queue queue, Segment segment, ByteBuffer buffer){
        Group group = topic.getGroup(queue.getId());
        AbstractMedium mm = loadMedium(topic, queue, group, segment);
        if (mm != null){
            mm.write(buffer);
        }
    }

    private AbstractMedium loadMedium(Topic topic, Queue queue, Group group, Segment segment){
        if (heap == null){
            return loadDram(topic, queue, group, segment);
        }
        return loadPMem(topic, queue, group, segment);
    }

    private AbstractMedium loadDram(Topic topic, Queue queue, Group group, Segment segment){
        return dram.computeIfAbsent(new Triple<>(topic.getName(), queue.getId(), segment.getIdx()), k -> new Dram(segment.load(group.getDb()), segment.getBeg()));
    }

    private AbstractMedium loadPMem(Topic topic, Queue queue, Group group, Segment segment){
        return pMem.computeIfAbsent(new Triple<>(topic.getName(), queue.getId(), segment.getIdx()), k -> {
            LOGGER.info("load Pmem pos {}, aos {}, topic {}, queue {}, idx {}", segment.getPos(), segment.getAos(), topic.getName(), queue.getId(), segment.getIdx());
            byte[] bytes = segment.loadBytes(group.getDb());
            AnyMemoryBlock anyMemoryBlock = heap.allocateMemoryBlock(blockSize);
            if (bytes != null && bytes.length > 0){
                anyMemoryBlock.copyFromArray(bytes, 0, 0, bytes.length);
            }
            return new PMem(anyMemoryBlock, segment.getBeg(), segment.getAos() - segment.getPos());
        });
    }
}
