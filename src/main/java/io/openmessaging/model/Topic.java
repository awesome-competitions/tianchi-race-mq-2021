package io.openmessaging.model;

import io.openmessaging.cache.Cache;
import io.openmessaging.cache.Storage;
import io.openmessaging.consts.Const;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
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

    public List<ByteBuffer> read(int queueId, long offset, int num) throws IOException {
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
            Storage storage = cache.loadStorage(group, readable.getSegment());
            if (storage == null) {
                break;
            }
            buffers.addAll(storage.read(readable.getStartOffset(), readable.getEndOffset()));
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
        int offset = queue.getAndIncrementOffset();

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

        cache.write(head, bytes);
        head.setEnd(offset);
        head.write(group.getDb(), wrapper);
        return offset;
    }

}