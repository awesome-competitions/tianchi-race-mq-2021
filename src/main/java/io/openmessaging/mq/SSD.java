package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SSD extends Data{

    private long start;

    private long end;

    private long position;

    private long capacity;

    private List<Long> sizes;

    public SSD(long start, long end, long position, long capacity, List<Long> blocks) {
        this.start = start;
        this.end = end;
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

    public List<Data> load(Heap heap, FileWrapper fw) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate((int) capacity);
        fw.read(position, buffer);
        buffer.flip();
        List<Data> list = new ArrayList<>();
        for (int i = 0; i < sizes.size(); i ++){
            long cap = sizes.get(i);
            byte[] bytes = new byte[(int) cap];
            buffer.get(bytes);

            Data data = null;
            if (heap == null){
                data = new Dram(ByteBuffer.wrap(bytes));
            }else{
                AnyMemoryBlock block = heap.allocateMemoryBlock(bytes.length);
                block.copyFromArray(bytes, 0, 0, bytes.length);
                data = new PMem(block);
            }
            data.setKey(new Key(key.getTopic(), key.getQueueId(), start + i));
            list.add(data);
        }
        return list;
    }

}
