package io.openmessaging.mq;

import io.openmessaging.consts.Const;
import io.openmessaging.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class Queue {

    private long offset;

    private final Map<Long, Data> records;

    private final Cache cache;

    private final FileWrapper fw;

    private final static Logger LOGGER = LoggerFactory.getLogger(Queue.class);

    public Queue(Cache cache, FileWrapper fw) {
        this.fw = fw;
        this.cache = cache;
        this.offset = -1;
        this.records = new HashMap<>();
    }

    public long nextOffset(){
        return ++ offset;
    }

    public long write(long position, ByteBuffer buffer){
        Data data = cache.allocate(position, (int) (Const.K * 17));
        if(data != null){
            data.set(buffer);
            records.put(offset, data);
            return offset;
        }
        records.put(offset, new SSD(fw, position, buffer.limit()));
        return offset;
    }

    public List<ByteBuffer> read(long offset, int num){
        List<ByteBuffer> buffers = new ArrayList<>();
        for (long i = offset; i < offset + num; i ++){
            Data data = records.get(i);
            if (data == null){
                break;
            }
            buffers.add(data.get());
            cache.recycle(data);
        }
        return buffers;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
