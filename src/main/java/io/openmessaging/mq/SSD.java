package io.openmessaging.mq;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class SSD extends Data{

    private final FileWrapper fw;

    public SSD(FileWrapper fw, long position, int capacity) {
        super(capacity);
        this.fw = fw;
        this.position = position;
    }

    @Override
    public ByteBuffer get() {
        ByteBuffer data = ByteBuffer.allocate(capacity);
        try {
            fw.read(position, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        data.flip();
        Monitor.readDistCount ++;
        return data;
    }

    @Override
    public void set(ByteBuffer buffer) {
        throw new UnsupportedOperationException("ssd not supported set");
    }

    @Override
    public void clear() {}
}
