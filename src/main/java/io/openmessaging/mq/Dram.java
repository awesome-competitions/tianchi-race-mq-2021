package io.openmessaging.mq;

import io.openmessaging.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Dram extends Data {

    private final List<ByteBuffer> buffers;

    public Dram(int capacity) {
        super(capacity);
        this.buffers = new ArrayList<>();
    }

    @Override
    public boolean writable(int size) {
        return capacity - position >= size;
    }

    @Override
    public void write(ByteBuffer buffer) {
        ByteBuffer data = ByteBuffer.allocate(buffer.capacity());
        data.put(buffer);
        data.flip();

        buffers.add(data);
        records.add(data.capacity());
        position += data.capacity();
        end = start + records.size() - 1;
    }

    @Override
    public List<ByteBuffer> read(long offset, int num) {
        if (offset > end){
            return null;
        }
        long startIndex = Math.max(offset, start) - start;
        long endIndex = Math.min(offset + num - 1, end) - start;
        List<ByteBuffer> results = new ArrayList<>();
        while(startIndex <= endIndex){
            ByteBuffer data = buffers.get((int) startIndex);
            ByteBuffer buffer = ByteBuffer.allocate(data.capacity());
            buffer.put(data);
            buffer.flip();
            data.flip();
            results.add(buffer);
            startIndex ++;
        }
        return results;
    }

    @Override
    public ByteBuffer load() {
        ByteBuffer buffer = ByteBuffer.allocate((int) position);
        for (ByteBuffer data: buffers){
            buffer.put(data);
            data.flip();
        }
        buffer.flip();
        return buffer;
    }

    @Override
    public long size() {
        return capacity;
    }

    @Override
    public void reset(long start, long end, long position, ByteBuffer buffer, List<Integer> records) {
        this.start = start;
        this.end = end;
        this.position = position;
        this.buffers.clear();
        this.records.clear();
        if (CollectionUtils.isNotEmpty(records)){
            this.records.addAll(records);
            for (Integer record: records){
                byte[] bytes = new byte[record];
                buffer.get(bytes);
                this.buffers.add(ByteBuffer.wrap(bytes));
            }
        }
    }

}
