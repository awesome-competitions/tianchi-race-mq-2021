package io.openmessaging.mq;

import io.openmessaging.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class PMem extends Data {

    private final Block block;

    private ByteBuffer ext;

    private int size;

    public PMem(Block block, long position, int capacity) {
        super(capacity);
        this.position = position;
        this.block = block;
        this.size = capacity;
        this.isPMem = true;
    }

    @Override
    public ByteBuffer get() {
        return get(Threads.get());
    }

    @Override
    public ByteBuffer get(Threads.Context ctx) {
        Monitor.readMemCount ++;
        int extSize = ext == null ? 0 : ext.limit();

        ByteBuffer buffer = ctx.allocateBuffer();
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

    public FileChannel getChannel(){
        return block.getFw().getChannel();
    }

}
