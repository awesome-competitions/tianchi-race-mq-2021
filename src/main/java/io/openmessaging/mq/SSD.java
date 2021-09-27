package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SSD extends Data{

    private final long start;

    private final long position;

    private final long capacity;

    private final List<Long> sizes;

    public SSD(long start, long position, long capacity, List<Long> blocks) {
        this.start = start;
        this.position = position;
        this.capacity = capacity;
        this.sizes = blocks;
    }

    @Override
    public ByteBuffer get() {
        throw new UnsupportedOperationException("ssd unsupported get");
    }

    @Override
    public void set(ByteBuffer buffer) {
        throw new UnsupportedOperationException("ssd unsupported set");
    }

    @Override
    public void clear() {

    }

    @Override
    public long size() {
        return capacity;
    }

    public List<ByteBuffer> load(long startOffset, FileWrapper fw) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate((int) capacity);
        fw.read(position, buffer);
        buffer.flip();
        List<ByteBuffer> list = new ArrayList<>();
        for (int i = 0; i < sizes.size(); i ++){
            int cap = sizes.get(i).intValue();
            long offset = start + i;
            if (offset < startOffset){
                buffer.position(buffer.position() + cap);
                continue;
            }
            byte[] bytes = new byte[cap];
            buffer.get(bytes);
            list.add(ByteBuffer.wrap(bytes));
        }
        return list;
    }

}
