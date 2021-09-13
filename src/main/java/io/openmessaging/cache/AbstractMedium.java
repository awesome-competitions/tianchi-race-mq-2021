package io.openmessaging.cache;

import io.openmessaging.model.FileWrapper;
import io.openmessaging.model.Segment;

import java.nio.ByteBuffer;
import java.util.List;

public abstract class AbstractMedium {

    public abstract List<ByteBuffer> read(long startOffset, long endOffset);

    public abstract void write(ByteBuffer buffer);

    public abstract void clean();

}
