package io.openmessaging.mq;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SSD extends Data{

    private final FileWrapper fw;

    public SSD(FileWrapper fw, long position, int capacity) {
        super(capacity);
        this.fw = fw;
        this.position = position;
        this.isSSD = true;
    }

    @Override
    public ByteBuffer get() {
        return get(Threads.get());
    }

    @Override
    public ByteBuffer get(Threads.Context ctx) {
        ByteBuffer buffer = ctx.allocateBuffer();
        buffer.limit(capacity);
        try {
            Monitor.readSSDCount ++;
            fw.read(position + 9, buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        buffer.flip();
        return buffer;
    }

    @Override
    public void set(ByteBuffer buffer) {
        throw new UnsupportedOperationException("ssd block not supported set");
    }

    @Override
    public void clear() {}

    public FileChannel getChannel(){
        return fw.getChannel();
    }
}
