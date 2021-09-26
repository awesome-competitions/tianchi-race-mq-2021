package io.openmessaging.mq;

import java.nio.ByteBuffer;

public abstract class Storage {

    public abstract ByteBuffer get();

    public abstract void set(ByteBuffer buffer);

    public abstract void clear();

    public abstract int size();

}
