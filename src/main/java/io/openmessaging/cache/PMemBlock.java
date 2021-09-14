package io.openmessaging.cache;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
import io.openmessaging.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.*;

public class PMemBlock extends Storage {

    private final AnyMemoryBlock block;

    private List<Long> positions;

    private long beginOffset;

    private long position;

    private int idx;

    public PMemBlock(AnyMemoryBlock block) {
        this.block = block;
    }

    public PMemBlock(AnyMemoryBlock block, List<ByteBuffer> buffers, long beginOffset) {
        this.block = block;
        reset(0, buffers, beginOffset);
    }

    short getShort(long pos){
        byte[] bytes = new byte[2];
        block.copyToArray(pos, bytes, 0, 2);
        return (short)((bytes[0] << 8) | (bytes[1] & 0xff));
    }

    @Override
    public List<ByteBuffer> read(long startOffset, long endOffset) {
        int startIndex = (int) (startOffset - beginOffset);
        int endIndex = (int) (endOffset - beginOffset);
        List<ByteBuffer> buffers = new ArrayList<>();
        long pos = positions.get(startIndex);
        while (startIndex <= endIndex && startIndex < positions.size()){
            int size = getShort(pos);
            pos += 2;
            byte[] bytes = new byte[size];
            block.copyToArray(pos, bytes, 0, bytes.length);
            pos += size;
            buffers.add(ByteBuffer.wrap(bytes));
            startIndex ++;
        }
        return buffers;
    }

    @Override
    public void write(byte[] bytes) {
        positions.add(position);
        block.setShort(position, (short) bytes.length);
        position += 2;
        block.copyFromArray(bytes, 0, position, bytes.length);
        position += bytes.length;
    }

    @Override
    public void clean() { }

    public void reset(int idx, List<ByteBuffer> buffers, long beginOffset) {
        this.idx = idx;
        long newPos = 0;
        if (CollectionUtils.isNotEmpty(buffers)) {
            for (ByteBuffer buffer : buffers) {
                positions.add(newPos);
                block.setShort(newPos, (short) buffer.capacity());
                newPos += 2;
                block.copyFromArray(buffer.array(), 0, newPos, buffer.capacity());
                newPos += buffer.capacity();
            }
        }
        this.position = newPos;
        this.beginOffset = beginOffset;
    }

    public long getIdx() {
        return idx;
    }
}
