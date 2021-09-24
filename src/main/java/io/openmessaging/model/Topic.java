package io.openmessaging.model;

import io.openmessaging.cache.Cache;
import io.openmessaging.cache.PMem;
import io.openmessaging.cache.Storage;
import io.openmessaging.consts.Const;
import io.openmessaging.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class Topic{
    private final String name;
    private final int id;
    private final Config config;
    private final Group[] groups;
    private final Map<Integer, Queue> queues;
    private final Cache cache;
    private final ReentrantLock lock;
    private final Aof aof;
    private final Map<Segment, List<ByteBuffer>> buffers;
    private ByteBuffer tempBuffer;

    private static final Logger LOGGER = LoggerFactory.getLogger(Topic.class);

    public Topic(String name, Integer id, Config config, Cache cache, Aof aof) throws IOException {
        this.name = name;
        this.id = id;
        this.config = config;
        this.queues = new HashMap<>();
        this.groups = new Group[config.getGroupSize()];
        this.cache = cache;
        this.lock = new ReentrantLock();
        this.aof = aof;
        this.buffers = new ConcurrentHashMap<>();
//        this.tempBuffer = ByteBuffer.allocate((int) (Const.K * 128));
//        new Thread(()->{
//            try{
//                lock.lock();
//                if (! buffers.isEmpty()){
//                    tempBuffer.clear();
//                    for (Map.Entry<Segment, List<ByteBuffer>> entry: buffers.entrySet()){
//                        Segment segment = entry.getKey();
//                        Group group = getGroup(segment.getQid());
//                        List<ByteBuffer> waitBuffers = entry.getValue();
//
//                        for (ByteBuffer waitBuffer: waitBuffers){
//                            if (waitBuffer.remaining() <= tempBuffer.remaining()){
//                                tempBuffer.put(waitBuffer);
//                            }else{
//                                waitBuffer.limit(tempBuffer.remaining());
//                                tempBuffer.put(waitBuffer);
//                                waitBuffer.limit(waitBuffer.capacity());
//                            }
//                            if (tempBuffer.remaining() == 0){
//                                tempBuffer.flip();
//                                segment.write(group.getDb(), tempBuffer);
//                                tempBuffer.clear();
//                            }
//                        }
//                    }
//                    buffers.clear();
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            } finally {
//                lock.unlock();
//            }
//        }).start();
    }

    static int hash(Object key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }

    private int indexFor(int queueId){
//        return hash(key) & (config.getGroupSize() - 1);
        return queueId % config.getGroupSize();
    }

    public Group getGroup(int queueId) throws IOException{
        int index = indexFor(queueId);
        Group group = groups[index];
        if (group == null){
            try{
                this.lock.lock();
                group = groups[index];
                if (group == null){
                    FileWrapper db = new FileWrapper(new RandomAccessFile(String.format(Const.DB_NAMED_FORMAT, config.getDataDir(), name, index), "rw"));
                    group = new Group(db, null);
//                    group.initQueues(this);
                    groups[index] = group;
                }
            }finally {
                this.lock.unlock();
            }
        }
        return group;
    }

    public Queue getQueue(int queueId){
        return queues.computeIfAbsent(queueId, Queue::new);
    }

    public List<ByteBuffer> read(int queueId, long offset, int num) throws IOException, InterruptedException {
        Queue queue = getQueue(queueId);
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
            List<ByteBuffer> data = cache.read(readable);
            if (CollectionUtils.isEmpty(data)){
                break;
            }
            buffers.addAll(data);
            queue.setLast(cache, readable.getSegment());
        }
        if (queue.getReadOffset() == 0 && offset != 0){
            cache.clearSegments(queue.getSegments(), queue.getLast());
        }
        queue.setReadOffset(offset + buffers.size());
        return buffers;
    }

    public long write(int queueId, ByteBuffer data) throws IOException, InterruptedException {
        Queue queue = getQueue(queueId);
        long offset = queue.getAndIncrementOffset();

        Segment head = queue.getHead();
        if (head == null || ! head.writable(data.capacity())){
            head = cache.applySegment(this, queue, offset);
        }
        head.setEnd(offset);
        cache.write(head, data);
        data.flip();

        ByteBuffer aofBuffer = ByteBuffer.allocate(5 + data.capacity())
                .put((byte) id)
                .putShort((short) queueId)
                .putShort((short) data.capacity())
                .put(data);
        aofBuffer.flip();
        aof.write(aofBuffer);
        return offset;
    }

    public String getName() {
        return name;
    }

    public int getId() {
        return id;
    }

    public long getPageSize(){
        return config.getPageSize();
    }
}