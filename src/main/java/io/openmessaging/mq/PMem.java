package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
import io.openmessaging.utils.CollectionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

public class PMem extends Data {

    private final Future<AnyMemoryBlock> block;

    public PMem(Future<AnyMemoryBlock> block, int capacity) {
        super(capacity);
        this.block = block;
    }

    @Override
    public boolean writable(int size) {
        return capacity - position >= size;
    }

    @Override
    public void write(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.capacity()];
        buffer.get(bytes);
        try {
            block.get().copyFromArray(bytes, 0, position, bytes.length);
            position += bytes.length;
            records.add(bytes.length);
            end = start + records.size() - 1;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<ByteBuffer> read(long offset, int num) {
        if (CollectionUtils.isEmpty(records) || offset > end){
            return null;
        }
        long startIndex = Math.max(offset, start) - start;
        long endIndex = Math.min(offset + num - 1, end) - start;
        long startPos = 0;
        long capacity = 0;
        for (int i = 0; i < startIndex; i ++){
            startPos += records.get(i);
        }
        for (int i = (int) startIndex; i <= endIndex; i ++){
            capacity += records.get(i);
        }
        byte[] bytes = new byte[(int) capacity];
        try {
            block.get().copyToArray(startPos, bytes, 0, bytes.length);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        ByteBuffer data = ByteBuffer.wrap(bytes);
        List<ByteBuffer> buffers = new ArrayList<>();
        while(startIndex <= endIndex){
            bytes = new byte[records.get((int) startIndex)];
            data.get(bytes);
            buffers.add(ByteBuffer.wrap(bytes));
            startIndex ++;
        }
        return buffers;
    }

    @Override
    public ByteBuffer load() {
        byte[] bytes = new byte[(int) position];
        try {
            block.get().copyToArray(0, bytes, 0, bytes.length);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public void reset(long start, long end, long position, ByteBuffer buffer, List<Integer> records) {
        this.start = start;
        this.end = end;
        this.position = position;
        this.records.clear();
        if (CollectionUtils.isNotEmpty(records)){
            this.records.addAll(records);
            try {
                byte[] bytes = new byte[buffer.capacity()];
                buffer.get(bytes);
                block.get().copyFromArray(bytes, 0, 0, bytes.length);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public long size() {
        return capacity;
    }
}
