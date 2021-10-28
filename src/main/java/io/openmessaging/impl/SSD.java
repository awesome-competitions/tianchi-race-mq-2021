package io.openmessaging.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class SSD extends Data{

    private final Aof fw;

    public SSD(Aof fw, long position, int capacity) {
        super(capacity);
        this.fw = fw;
        this.position = position;
        this.isSSD = true;
    }

    @Override
    public ByteBuffer get(ByteBuffer buffer) {
        buffer.limit(capacity);
        try {
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
