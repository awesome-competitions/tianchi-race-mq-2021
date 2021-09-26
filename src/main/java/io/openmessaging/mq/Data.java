package io.openmessaging.mq;

import java.nio.ByteBuffer;

public abstract class Data {

    protected long offset;

    public abstract ByteBuffer get();

    public abstract void set(ByteBuffer buffer);

    public abstract void clear();

    public abstract int size();

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
