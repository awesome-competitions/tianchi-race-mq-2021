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

    private final Block block;

    private byte[] ext;

    private int size;

    public PMem(Block block, long position, int capacity) {
        super(capacity);
        this.position = position;
        this.block = block;
    }

    @Override
    public ByteBuffer get() {
        Monitor.readMemCount ++;
        byte[] data = block.read(position, size);
        ByteBuffer buffer = ByteBuffer.allocate(data.length + (ext != null ? ext.length : 0));
        buffer.put(data);
        if (ext != null){
            buffer.put(ext);
        }
        buffer.flip();
        return buffer;
    }

    @Override
    public void set(ByteBuffer buffer) {
        Monitor.writeMemCount ++;

        byte[] bytes = new byte[Math.min(buffer.limit(), capacity)];
        buffer.get(bytes);
        block.write(position, bytes);

        if (buffer.remaining() > 0){
            ext = new byte[buffer.remaining()];
            buffer.get(ext);
        }
        this.size = buffer.limit();
    }

    @Override
    public void clear() {
        this.ext = null;
    }

}
