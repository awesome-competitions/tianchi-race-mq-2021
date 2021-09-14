package io.openmessaging.cache;

import java.nio.ByteBuffer;
import java.util.List;

public class Dram extends Storage {

    private final List<ByteBuffer> data;

    private final long beginOffset;

    public Dram(List<ByteBuffer> data, long beginOffset) {
        this.data = data;
        this.beginOffset = beginOffset;
    }

    @Override
    public List<ByteBuffer> read(long startOffset, long endOffset) {
        return data.subList((int) (startOffset - beginOffset), (int) (endOffset - beginOffset + 1));
    }

    @Override
    public void write(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.flip();
        data.add(buffer);
    }

    @Override
    public void clean() {
        data.clear();
    }
}
