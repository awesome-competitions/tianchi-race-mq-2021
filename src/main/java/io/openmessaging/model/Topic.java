package io.openmessaging.model;

import io.openmessaging.cache.Cache;
import io.openmessaging.cache.Storage;
import io.openmessaging.consts.Const;
import io.openmessaging.utils.CollectionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Topic{
    private final String name;
    private final int id;
    private final Config config;
    private final List<Group> groups;
    private final Map<Integer, Queue> queues;
    private final Cache cache;

    public Topic(String name, Integer id, Config config, Cache cache) throws IOException {
        this.name = name;
        this.id = id;
        this.config = config;
        this.queues = new ConcurrentHashMap<>();
        this.groups = new ArrayList<>(config.getGroupSize());
        this.cache = cache;
        initGroups();
    }

    private void initGroups() throws IOException {
        for (int i = 0; i < config.getGroupSize(); i ++){
            FileWrapper db = new FileWrapper(Paths.get(String.format(Const.DB_NAMED_FORMAT, config.getDataDir(), name, i)));
            FileWrapper idx = new FileWrapper(Paths.get(String.format(Const.IDX_NAMED_FORMAT, config.getDataDir(), name, i)));
            Group group = new Group(db, idx);
            group.initQueues(this);
            groups.add(group);
        }
    }

    public Group getGroup(int queueId){
        return groups.get(queueId % config.getGroupSize());
    }

    public Queue getQueue(int queueId){
        return queues.computeIfAbsent(queueId, Queue::new);
    }

    public List<ByteBuffer> read(int queueId, long offset, int num) throws IOException, InterruptedException {
        Queue queue = getQueue(queueId);
        Group group = getGroup(queue.getId());
        Segment segment = queue.getLast(offset);
        if (segment == null){
            return null;
        }
        long startOffset = offset;
        long endOffset = offset + num - 1;
        List<Readable> readableList = new ArrayList<>();
        while (num > 0 && segment != null){
            if (segment.getStart() > startOffset || segment.getEnd() < startOffset){
                segment = queue.getSegment(startOffset);
                continue;
            }
            if (segment.getEnd() >= endOffset){
                readableList.add(new Readable(segment, startOffset, endOffset));
                break;
            }else {
                readableList.add(new Readable(segment, startOffset, segment.getEnd()));
                num -= segment.getEnd() - startOffset + 1;
                startOffset = segment.getEnd() + 1;
                segment = queue.nextSegment(segment);
            }
        }
        List<ByteBuffer> buffers = new ArrayList<>(num);
        for (Readable readable : readableList) {
            Storage storage = cache.loadStorage(this, queue, group, readable.getSegment());
            List<ByteBuffer> data = storage.read(readable.getStartOffset(), readable.getEndOffset());
            if (CollectionUtils.isEmpty(data)){
                break;
            }
            buffers.addAll(data);
            queue.setLast(readable.getSegment());
        }
        return buffers;
    }

    public long write(int queueId, ByteBuffer data) throws IOException{
        int n = data.remaining();
        byte[] bytes = new byte[n];
        for (int i = 0; i < n; i++){
            bytes[i] = data.get();
        }

        Queue queue = getQueue(queueId);
        Group group = getGroup(queueId);
        long offset = queue.getAndIncrementOffset();

        ByteBuffer wrapper = ByteBuffer.allocate(2 + data.capacity());
        wrapper.putShort((short) data.capacity());
        wrapper.put(bytes);
        wrapper.flip();

        Segment head = queue.getHead();
        if (head == null || ! head.writable(wrapper.capacity())){
            head = new Segment(offset, offset, (long) group.getAndIncrementOffset() * config.getPageSize(), config.getPageSize());
            queue.addSegment(head);
            ByteBuffer idxBuffer = ByteBuffer.allocate(26)
                    .putShort((short) queueId)
                    .putLong(head.getStart())
                    .putLong(head.getPos())
                    .putLong(head.getCap());
            idxBuffer.flip();
            group.getIdx().write(idxBuffer);
        }

        cache.write(queue, head, bytes);
        head.setEnd(offset);
        head.write(group.getDb(), wrapper);
        return offset;
    }

    public String getName() {
        return name;
    }

    public int getId() {
        return id;
    }
}