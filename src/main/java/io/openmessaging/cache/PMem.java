package io.openmessaging.cache;

import com.intel.pmem.llpl.AnyMemoryBlock;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PMem extends AbstractMedium{

    private final AnyMemoryBlock block;

    private final long beginOffset;

    private long position;

    public PMem(AnyMemoryBlock block, long beginOffset, long position) {
        this.block = block;
        this.beginOffset = beginOffset;
        this.position = position;
    }

    short getShort(long pos){
        byte[] bytes = new byte[2];
        block.copyToArray(pos, bytes, 0, 2);
        short s = (short)((bytes[0] << 8) | (bytes[1] & 0xff));
        if (s < 0){
            System.out.println(Arrays.toString(bytes));
        }
        return s;
    }

    @Override
    public List<ByteBuffer> read(long startOffset, long endOffset) {
        int startIndex = (int) (startOffset - beginOffset);
        int endIndex = (int) (endOffset - beginOffset);
        short size;
        long offset = 0;
        int currentIndex = 0;
        while (currentIndex < startIndex){
            size = getShort(offset);
            offset += 2 + size;
            currentIndex ++;
        }
        List<ByteBuffer> buffers = new ArrayList<>();
        while (startIndex <= endIndex){
            size = getShort(offset);
            offset += 2;
            byte[] bytes = new byte[size];
            block.copyToArray(offset, bytes, 0, size);
            buffers.add(ByteBuffer.wrap(bytes));
            startIndex ++;
        }
        return buffers;
    }

    @Override
    public void write(ByteBuffer buffer) {
        block.copyFromArray(buffer.array(), 0, position, buffer.capacity());
        position += buffer.capacity();
    }

    @Override
    public void clean() {
        if (block != null && block.isValid()){
            block.freeMemory();
        }
    }
}
