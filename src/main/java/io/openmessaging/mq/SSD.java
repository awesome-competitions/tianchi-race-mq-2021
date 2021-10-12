package io.openmessaging.mq;

import java.io.IOException;
import java.nio.ByteBuffer;
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
    }

    @Override
    public ByteBuffer get() {
        ByteBuffer buffer = Threads.get().allocateBuffer();
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
}
