package io.openmessaging.mq;

import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import io.openmessaging.consts.Const;
import io.openmessaging.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

public class Queue {

    private long offset;

    private Topic topic;

    private final Map<Long, Data> records;

    private final Cache cache;

    private final FileWrapper aof;

    private SSDBlock ssdBlock;

    private boolean reading;

    private final static Logger LOGGER = LoggerFactory.getLogger(Queue.class);

    public Queue(Topic topic, Cache cache, FileWrapper aof) {
        this.topic = topic;
        this.aof = aof;
        this.cache = cache;
        this.offset = -1;
        this.records = new HashMap<>();
    }

    public long nextOffset(){
        return ++ offset;
    }

    public long write(ByteBuffer buffer){
        Data data = cache.allocate(buffer.limit());
        if(data != null){
            data.set(buffer);
            records.put(offset, data);
            return offset;
        }
        if (ssdBlock == null || ! ssdBlock.writable(buffer)){
            ssdBlock = topic.nextSSDBlock();
        }
        ssdBlock.write(offset, buffer);
        records.put(offset, ssdBlock);
        return offset;
    }

    public List<ByteBuffer> read(long offset, int num){
        if (!reading){
            for (long i = 0; i < offset; i ++){
                Data data = records.remove(i);
                cache.recycle(data);
            }
            reading = true;
        }
        List<ByteBuffer> buffers = new ArrayList<>();
        Map<Long, Data> tmpRecords = new HashMap<>();
        for (long i = offset; i < offset + num; i ++){
            Data data = tmpRecords.remove(i);
            if (data == null){
                data = records.get(i);
                if (data == null){
                    break;
                }
            }
            if (data instanceof PMem){
                buffers.add(data.get());
                cache.recycle(data);
            }else if (data instanceof SSDBlock){
                SSDBlock ssdBlock = (SSDBlock) data;
                tmpRecords.putAll(ssdBlock.load());
                buffers.add(tmpRecords.remove(i).get());
            }else if (data instanceof Dram){
                buffers.add(data.get());
            }else if (data instanceof SSD){
                buffers.add(data.get());
            }
        }
        long nextReadOffset = offset + buffers.size();
        if (! tmpRecords.isEmpty()){
            for (Map.Entry<Long, Data> tmpRecord: tmpRecords.entrySet()){
                if (tmpRecord.getKey() < nextReadOffset){
                    continue;
                }
                Data data = cache.allocate(tmpRecord.getValue().getCapacity());
                if (data != null){
                    data.set(tmpRecord.getValue().get());
                    records.put(tmpRecord.getKey(), data);
                }
            }
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
