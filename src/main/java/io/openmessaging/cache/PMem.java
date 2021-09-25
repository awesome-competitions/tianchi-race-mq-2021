package io.openmessaging.cache;

import com.intel.pmem.llpl.AnyMemoryBlock;
import io.openmessaging.utils.CollectionUtils;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class PMem extends Storage {

    private AnyMemoryBlock block;

    private List<Long> positions;

    private long beginOffset;

    private long position;

    private int idx;

    public PMem(AnyMemoryBlock block) {
        this(block, null, 0);
    }

    public PMem(AnyMemoryBlock block, List<ByteBuffer> buffers, long beginOffset) {
        this.block = block;
        reset(-1, buffers, beginOffset);
    }

    @Override
    public List<ByteBuffer> read(long startOffset, long endOffset) {
        if (CollectionUtils.isEmpty(positions)){
            return null;
        }
        int startIndex = (int) (startOffset - beginOffset);
        int endIndex = (int) (endOffset - beginOffset);
        List<ByteBuffer> buffers = new ArrayList<>();
        long startPos = 0;
        for (int i = 0; i < startIndex; i ++){
            startPos += positions.get(i);
        }
        while (startIndex <= endIndex && startIndex < positions.size()){
            Long size = positions.get(startIndex);
            byte[] bytes = new byte[size.intValue()];
            block.copyToArray(startPos, bytes, 0, bytes.length);
            buffers.add(ByteBuffer.wrap(bytes));
            startIndex ++;
            startPos += bytes.length;
        }
        return buffers;
    }

    @Override
    public List<ByteBuffer> load() {
        if (CollectionUtils.isEmpty(positions)){
            return null;
        }
        int startIndex = 0;
        long startPos = 0;
        List<ByteBuffer> buffers = new ArrayList<>();
        while (startIndex < positions.size()){
            Long size = positions.get(startIndex);
            byte[] bytes = new byte[size.intValue()];
            block.copyToArray(startPos, bytes, 0, bytes.length);
            buffers.add(ByteBuffer.wrap(bytes));
            startIndex ++;
            startPos += bytes.length;
        }
        return buffers;
    }

    @Override
    public void write(ByteBuffer byteBuffer) {
        byte[] bytes = new byte[byteBuffer.capacity()];
        byteBuffer.get(bytes);
        positions.add((long) bytes.length);
        block.copyFromArray(bytes, 0, position, bytes.length);
        position += bytes.length;
    }

    @Override
    public void reset(int idx, List<ByteBuffer> buffers, long beginOffset) {
        this.idx = idx;
        long newPos = 0;
        if (positions == null){
            positions = new ArrayList<>();
        }
        positions.clear();
        if (CollectionUtils.isNotEmpty(buffers)) {
            for (ByteBuffer buffer : buffers) {
                block.copyFromArray(buffer.array(), 0, newPos, buffer.capacity());
                positions.add((long) buffer.capacity());
                newPos += buffer.capacity();
            }
        }
        this.position = newPos;
        this.beginOffset = beginOffset;
    }

    @Override
    public long getIdx() {
        return idx;
    }

    @Override
    public void clean() {
        if (block != null && block.isValid()){
            block.freeMemory();
            this.block = null;
        }
    }

    @Override
    public String toString() {
        return "PMem{" +
                "block=" + block +
                ", positions=" + positions +
                ", beginOffset=" + beginOffset +
                ", position=" + position +
                ", idx=" + idx +
                '}';
    }
}
