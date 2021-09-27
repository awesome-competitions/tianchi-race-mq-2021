package io.openmessaging.mq;

import java.nio.ByteBuffer;

public abstract class Data {

    protected Key key;

    public abstract ByteBuffer get();

    public abstract void set(ByteBuffer buffer);

    public abstract void clear();

    public abstract long size();

    public Key getKey() {
        return key;
    }

    public void setKey(Key key) {
        this.key = key;
    }
}
