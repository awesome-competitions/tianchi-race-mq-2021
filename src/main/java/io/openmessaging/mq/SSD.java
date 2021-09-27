package io.openmessaging.mq;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class SSD extends Data{

    private final FileWrapper fw;

    public SSD(long start, long end, long position, int capacity, FileWrapper fw, List<Integer> records) {
        super(capacity);
        this.fw = fw;
        this.start = start;
        this.end = end;
        this.position = position;
        this.records = new ArrayList<>(records);
    }

    @Override
    public boolean writable(int size) {
        return false;
    }

    @Override
    public void write(ByteBuffer buffer) {
        throw new UnsupportedOperationException("ssd unsupported write");
    }

    @Override
    public List<ByteBuffer> read(long offset, int num) {
        if (offset > end){
            return null;
        }
        long startIndex = Math.max(offset, start) - start;
        long endIndex = Math.min(offset + num - 1, end) - start;
        long startPos = position;
        long capacity = 0;
        for (int i = 0; i < startIndex; i ++){
            startPos += records.get(i);
        }
        for (int i = (int) startIndex; i <= endIndex; i ++){
            capacity += records.get(i);
        }
        ByteBuffer data = ByteBuffer.allocate((int) capacity);
        try {
            fw.read(startPos, data);
            data.flip();
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<ByteBuffer> buffers = new ArrayList<>();
        while(startIndex <= endIndex){
            byte[] bytes = new byte[records.get((int) startIndex)];
            data.get(bytes);
            buffers.add(ByteBuffer.wrap(bytes));
            startIndex ++;
        }
        return buffers;
    }

    @Override
    public ByteBuffer load() {
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        try {
            fw.read(position, buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer;
    }

    @Override
    public long size() {
        return capacity;
    }

    @Override
    public void reset(long start, long end, long position, ByteBuffer buffer, List<Integer> records) {
        throw new UnsupportedOperationException("ssd unsupported reset");
    }
}
