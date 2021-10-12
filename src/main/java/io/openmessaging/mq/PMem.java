package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
import io.openmessaging.utils.BufferUtils;
import io.openmessaging.utils.CollectionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

public class PMem extends Data {

    private final Block block;

    private ByteBuffer ext;

    private int size;

    public PMem(Block block, long position, int capacity) {
        super(capacity);
        this.position = position;
        this.block = block;
        this.size = capacity;
    }

    @Override
    public ByteBuffer get() {
        Monitor.readMemCount ++;
        int extSize = ext == null ? 0 : ext.limit();

        ByteBuffer buffer = Threads.get().allocateBuffer();
        buffer.limit(size - extSize);
        block.read(position, buffer);
        if (extSize > 0){
            buffer.limit(size);
            buffer.put(ext);
        }
        buffer.flip();
        return buffer;
    }

    @Override
    public void set(ByteBuffer buffer) {
        Monitor.writeMemCount ++;
        this.size = buffer.limit();

        buffer.limit(Math.min(size, capacity));
        block.write(position, buffer);

        if (size > capacity){
            buffer.limit(size);
            ext = ByteBuffer.allocateDirect(buffer.remaining());
            Monitor.extSize += ext.capacity();
            ext.put(buffer);
            ext.flip();
        }
    }

    @Override
    public void clear() {
        if (this.ext != null){
            Monitor.extSize -= ext.capacity();
            BufferUtils.clean(ext);
        }
        this.ext = null;
    }

}
