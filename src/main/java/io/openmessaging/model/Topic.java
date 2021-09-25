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
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class Topic{
    private final String name;
    private final int id;
    private final Config config;
    private final Group[] groups;
    private final Queue[] queues;
    private final Cache cache;
    private final ReentrantLock lock;
    private final Aof aof;
    private final Map<Segment, List<ByteBuffer>> buffers;
    private ByteBuffer tempBuffer;
    private CyclicBarrier cyclicBarrier;

    private static final Logger LOGGER = LoggerFactory.getLogger(Topic.class);

    public Topic(String name, Integer id, Config config, Cache cache, Aof aof, CyclicBarrier cyclicBarrier) throws IOException {
        this.name = name;
        this.id = id;
        this.config = config;
        this.queues = new Queue[5001];
        this.groups = new Group[config.getGroupSize()];
        this.cache = cache;
        this.lock = new ReentrantLock();
        this.aof = aof;
        this.buffers = new ConcurrentHashMap<>();
        this.cyclicBarrier = cyclicBarrier;
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
        Queue queue = queues[queueId];
        if (queue == null){
            queue = new Queue(queueId);
            queues[queueId] = queue;
        }
        return queue;
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
            try{
                List<ByteBuffer> data = cache.read(readable);
                if (CollectionUtils.isEmpty(data)){
                    break;
                }
                buffers.addAll(data);
                queue.setLast(readable.getSegment());
            }catch (IndexOutOfBoundsException e){
                Segment head = readable.getSegment();
                LOGGER.info("err read topic {}, queue {}, segment {}, pos {}, cap {}, offset {}, stroage: {}", this.id, queueId, head, head.getPos(), head.getCap(), offset, head.getStorage());
                throw e;
            }
        }
        queue.setReadOffset(offset + buffers.size());
        return buffers;
    }

    public long write(int queueId, ByteBuffer data) throws IOException, InterruptedException {
        Queue queue = getQueue(queueId);
        long offset = queue.getAndIncrementOffset();
//        Segment head = queue.getHead();
//        if (head == null || ! head.writable(data.capacity())){
//            head = cache.applySegment(this, queue, offset);
//        }
//        head.setEnd(offset);
//        try{
//            cache.write(head, data);
//        }catch (IndexOutOfBoundsException e){
//            LOGGER.info("err write topic {}, queue {}, segment {}, pos {}, cap {}, len {}, stroage: {}", this.id, queueId, head, head.getPos(), head.getCap(), data.capacity(), head.getStorage());
//            throw e;
//        }
//        data.flip();
        ByteBuffer header = ByteBuffer.allocateDirect(5)
                .put((byte) id)
                .putShort((short) queueId)
                .putShort((short) data.capacity());
        header.flip();
        try {
            this.aof.getWrapper().write(new ByteBuffer[]{header, data});
            cyclicBarrier.await(10, TimeUnit.SECONDS);
        } catch (BrokenBarrierException | TimeoutException e) {
            e.printStackTrace();
            this.aof.getWrapper().getChannel().force(false);
        }
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