package io.openmessaging.model;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryAccessor;
import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import org.omg.CORBA.Any;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Cache {

    private Heap heap;

    private int blockSize;

    private Lru<Triple<String, Integer, Long>, AnyMemoryBlock> pMemBlocks;

    private Lru<Triple<String, Integer, Long>, List<ByteBuffer>> ssdBlocks;

    public Cache(String path, long heapSize, int cacheSize, int blockSize){
        if (Objects.nonNull(path)){
            this.heap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
            this.pMemBlocks = new Lru<>(cacheSize, v -> {
                if(v.isValid()){
                    v.freeMemory();    
                }
            });
        }else{
            this.ssdBlocks = new Lru<>(cacheSize, v -> {});
        }
        this.blockSize = blockSize;
    }

    public List<ByteBuffer> computeIfAbsent(Triple<String, Integer, Long> key, Segment segment, FileWrapper fw){
        if (heap == null){
            return computeIfAbsentSSD(key, segment, fw);
        }
        return computeIfAbsentPEME(key, segment, fw);
    }

    private List<ByteBuffer> computeIfAbsentSSD(Triple<String, Integer, Long> key, Segment segment, FileWrapper fw){
        return ssdBlocks.computeIfAbsent(key, k -> segment.getData(fw));
    }

    private List<ByteBuffer> computeIfAbsentPEME(Triple<String, Integer, Long> key, Segment segment, FileWrapper fw){
        List<ByteBuffer> results = new ArrayList<>();
        AnyMemoryBlock block = pMemBlocks.computeIfAbsent(key, k -> {
            List<ByteBuffer> buffers = segment.getData(fw);
            if (buffers == null){
                return null;
            }
            results.addAll(buffers);

            AnyMemoryBlock anyMemoryBlock = heap.allocateMemoryBlock(bytes.length);
            for( int i = 0; i < bytes.length; i ++){
                anyMemoryBlock.setByte(i, bytes[i]);
            }
            return anyMemoryBlock;


            long startOffset = segment.getBeg();
            AnyMemoryBlock curr = null;
            for (byte[] bytes: data){
                AnyMemoryBlock currBlock = pMemBlocks.computeIfAbsent(new Triple<>(key.getK1(), key.getK2(), startOffset), k1 -> {
                    AnyMemoryBlock anyMemoryBlock = heap.allocateMemoryBlock(bytes.length);
                    for( int i = 0; i < bytes.length; i ++){
                        anyMemoryBlock.setByte(i, bytes[i]);
                    }
                    return anyMemoryBlock;
                });
                if (Objects.equals(startOffset, key.getK3())){
                    curr = currBlock;
                }
                startOffset ++;
            }
            return curr;
        });

        ByteBuffer buffer = ByteBuffer.allocate((int) block.size());
        for (int i = 0; i < block.size(); i ++){
            buffer.put(block.getByte(i));
        }
        return buffer;
    }
}
