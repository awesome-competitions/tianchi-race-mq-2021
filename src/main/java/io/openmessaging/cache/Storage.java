package io.openmessaging.cache;

import io.openmessaging.model.FileWrapper;
import io.openmessaging.model.Segment;

import java.nio.ByteBuffer;
import java.util.List;

public abstract class Storage {

    protected long expire;

    protected boolean direct;

    public abstract List<ByteBuffer> read(long startOffset, long endOffset);

    public abstract List<ByteBuffer> load();

    public abstract void write(ByteBuffer byteBuffer);

    public abstract void reset(int idx, List<ByteBuffer> buffers, long beginOffset);

    public abstract long getIdx();

    public abstract void clean();

    public void killed(){
        this.expire = System.currentTimeMillis() + 1000;
    }

    public long expire(){
        return expire;
    }

    public boolean isDirect() {
        return direct;
    }
}
