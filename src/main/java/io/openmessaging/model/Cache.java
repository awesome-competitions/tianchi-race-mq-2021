package io.openmessaging.model;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryAccessor;
import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import org.omg.CORBA.Any;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class Cache {

    private Heap heap;

    private Lru<Triple<String, Integer, Long>, AnyMemoryBlock> pMemBlocks;

    private Lru<Triple<String, Integer, Long>, ByteBuffer> ssdBlocks;

    public Cache(String path, long heapSize, int cacheSize){
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
    }

    public ByteBuffer computeIfAbsent(Triple<String, Integer, Long> key, Segment segment, FileWrapper fw){
        if (heap == null){
            return computeIfAbsentSSD(key, segment, fw);
        }
        return computeIfAbsentPEME(key, segment, fw);
    }

    private ByteBuffer computeIfAbsentSSD(Triple<String, Integer, Long> key, Segment segment, FileWrapper fw){
        return ssdBlocks.computeIfAbsent(key, k -> {
            List<byte[]> data = segment.getData(fw);
            long startOffset = segment.getBeg();
            ByteBuffer buffer = null;
            for (byte[] bytes: data){
                ssdBlocks.computeIfAbsent(new Triple<>(key.getK1(), key.getK2(), startOffset), k1 -> ByteBuffer.wrap(bytes));
                if (Objects.equals(startOffset, key.getK3())){
                    buffer = ByteBuffer.wrap(bytes);
                }
                startOffset ++;
            }
            return buffer;
        });
    }

    private ByteBuffer computeIfAbsentPEME(Triple<String, Integer, Long> key, Segment segment, FileWrapper fw){
        AnyMemoryBlock block = pMemBlocks.computeIfAbsent(key, k -> {
            List<byte[]> data = segment.getData(fw);
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
