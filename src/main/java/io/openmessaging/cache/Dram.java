package io.openmessaging.cache;

import java.nio.ByteBuffer;
import java.util.List;

public class Dram extends Storage {

    private List<ByteBuffer> data;

    private long beginOffset;

    private int idx;

    public Dram() {
        this.idx = -1;
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
    public void reset(int idx, List<ByteBuffer> buffers, long beginOffset) {
        this.idx = idx;
        this.data = buffers;
        this.beginOffset = beginOffset;
    }

    @Override
    public long getIdx() {
        return idx;
    }

    @Override
    public void clean() {
        this.data = null;
    }


}
