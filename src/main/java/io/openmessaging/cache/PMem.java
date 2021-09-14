package io.openmessaging.cache;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
import io.openmessaging.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PMem extends Storage {

    private final List<AnyMemoryBlock> blocks;

    private final long beginOffset;

    private final Heap heap;

    public PMem(Heap heap, List<AnyMemoryBlock> blocks, long beginOffset) {
        this.blocks = blocks;
        this.beginOffset = beginOffset;
        this.heap = heap;
    }

    @Override
    public List<ByteBuffer> read(long startOffset, long endOffset) {
        int startIndex = (int) (startOffset - beginOffset);
        int endIndex = (int) (endOffset - beginOffset);
        List<ByteBuffer> buffers = new ArrayList<>();
        while (startIndex <= endIndex && startIndex < blocks.size()){
            AnyMemoryBlock block = blocks.get(startIndex);
            byte[] bytes = new byte[(int) block.size()];
            block.copyToArray(0, bytes, 0, bytes.length);
            buffers.add(ByteBuffer.wrap(bytes));
            startIndex ++;
        }
        return buffers;
    }

    @Override
    public void write(byte[] bytes) {
        AnyMemoryBlock anyMemoryBlock = heap.allocateMemoryBlock(bytes.length);
        anyMemoryBlock.copyFromArray(bytes, 0, 0, bytes.length);
        blocks.add(anyMemoryBlock);
    }

    @Override
    public void clean() {
        if (CollectionUtils.isNotEmpty(blocks)){
            try{
                blocks.stream().filter(AnyMemoryBlock::isValid).forEach(AnyMemoryBlock::freeMemory);
            }catch (IllegalStateException e){}
        }
    }
}
