package io.openmessaging.model;

import io.openmessaging.cache.Cache;
import io.openmessaging.consts.Const;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Topic{
    private final String name;
    private final Config config;
    private final List<Group> groups;
    private final Map<Integer, Queue> queues;
    private final Cache cache;

    public Topic(String name, Config config, Cache cache) throws FileNotFoundException {
        this.name = name;
        this.config = config;
        this.queues = new ConcurrentHashMap<>();
        this.groups = new ArrayList<>(config.getGroupSize());
        this.cache = cache;
        initGroups();
    }

    private void initGroups() throws FileNotFoundException {
        for (int i = 0; i < config.getGroupSize(); i ++){
            FileWrapper db = new FileWrapper(new RandomAccessFile(String.format(Const.DB_NAMED_FORMAT, config.getDataDir(), name, i), "rw"));
            FileWrapper idx = new FileWrapper(new RandomAccessFile(String.format(Const.IDX_NAMED_FORMAT, config.getDataDir(), name, i), "rw"));
            groups.add(new Group(db, idx));
        }
    }

    public Group getGroup(int queueId){
        return groups.get(queueId % config.getGroupSize());
    }

    public Queue getQueue(int queueId){
        return queues.computeIfAbsent(queueId, Queue::new);
    }

    public String getName(){
        return name;
    }

    public List<ByteBuffer> read(int queueId, long offset, int num) throws IOException {
        return cache.load(this, getQueue(queueId), offset, num);
    }

    public long write(int queueId, ByteBuffer data) throws IOException{
        Queue queue = getQueue(queueId);
        Group group = getGroup(queueId);
        int offset = queue.getAndIncrementOffset();

        ByteBuffer wrapper = ByteBuffer.allocate(2 + data.capacity());
        wrapper.putShort((short) data.capacity());
        wrapper.put(data);
        wrapper.flip();
        wrapper.mark();

        Segment last = queue.getLast();
        if (last == null || ! last.writable(wrapper.capacity())){
            last = new Segment(offset, offset, (long) group.getAndIncrementOffset() * config.getPageSize(), config.getPageSize());
            queue.addSegment(last);
            ByteBuffer idxBuffer = ByteBuffer.allocate(26)
                    .putShort((short) queueId)
                    .putLong(last.getBeg())
                    .putLong(last.getPos())
                    .putLong(last.getCap());
            idxBuffer.flip();
            group.getIdx().write(idxBuffer);
        }
        last.setEnd(offset);
        last.write(group.getDb(), wrapper);
        wrapper.reset();
        cache.write(this, queue, last, wrapper);
        return offset;
    }

}