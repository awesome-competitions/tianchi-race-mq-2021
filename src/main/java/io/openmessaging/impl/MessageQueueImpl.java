package io.openmessaging.impl;

import io.openmessaging.MessageQueue;
import io.openmessaging.model.*;
import io.openmessaging.model.Queue;
import io.openmessaging.utils.ArrayUtils;
import io.openmessaging.utils.BufferUtils;
import io.openmessaging.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageQueueImpl extends MessageQueue {

    private static final String DB_NAMED_FORMAT = "%s%s_%d.db";
    private static final String IDX_NAMED_FORMAT = "%s%s_%d.idx";
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageQueueImpl.class);

    private final Config config;
    private final Map<String, Topic> topics;

    public MessageQueueImpl() {
//        this("/essd/", 1024 * 1024 * 16, 10);
        this(new Config("/essd/", 1024 * 1024 * 16, 1, 10, 10000));
    }

    public MessageQueueImpl(Config config) {
        this.config = config;
        this.topics = new ConcurrentHashMap<>();
    }

    public void loadDB(){}

    public void cleanDB(){
        File root = new File(config.getDataDir());
        if (root.exists() && root.isDirectory()){
            if (ArrayUtils.isEmpty(root.listFiles())) return;
            for (File file: Objects.requireNonNull(root.listFiles())){
                if (file.exists() && ! file.isDirectory() && file.delete()){ }
            }
        }
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        try {
            return getTopic(topic).write(queueId, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String name, int queueId, long offset, int fetchNum) {
        try {
            Topic topic = getTopic(name);
            List<ByteBuffer> results = topic.read(queueId, offset, fetchNum);
            if (CollectionUtils.isEmpty(results)){
                return null;
            }
            Map<Integer, ByteBuffer> byteBuffers = new HashMap<>();
            for(int i = 0; i < results.size(); i ++){
                byteBuffers.put(i, results.get(i));
            }
            return byteBuffers;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public synchronized Topic getTopic(String name) throws FileNotFoundException {
        Topic topic = topics.get(name);
        if (topic == null){
            topic = new Topic(name, config);
            topics.put(name, topic);
        }
        return topic;
    }

    public static class Topic{
        private final String name;
        private final Config config;
        private final List<Group> groups;
        private final Map<Integer, Queue> queues;
        private final Lru<Tuple<Integer, Integer>, List<ByteBuffer>> caches;

        public Topic(String name, Config config) throws FileNotFoundException {
            this.name = name;
            this.config = config;
            this.queues = new ConcurrentHashMap<>();
            this.caches = new Lru<>(config.getCacheSize(), x->{});
            this.groups = new ArrayList<>(config.getGroupSize());
            initGroups();
        }

        private void initGroups() throws FileNotFoundException {
            for (int i = 0; i < config.getGroupSize(); i ++){
                FileWrapper db = new FileWrapper(new RandomAccessFile(String.format(DB_NAMED_FORMAT, config.getDataDir(), name, i), "rwd"));
                FileWrapper idx = new FileWrapper(new RandomAccessFile(String.format(IDX_NAMED_FORMAT, config.getDataDir(), name, i), "rwd"));
                groups.add(new Group(db, idx));
            }
        }

        private Group getQueueGroup(Queue queue){
            return groups.get(queue.getId() % config.getGroupSize());
        }

        public Queue getQueue(int queueId){
            return queues.computeIfAbsent(queueId, Queue::new);
        }

        public List<ByteBuffer> read(int queueId, long offset, int num) throws IOException {
            Queue queue = getQueue(queueId);
            Segment seg = queue.search(offset);
            if (seg == null){
                return null;
            }
            List<ByteBuffer> data = readData(queue, seg);
            if (data == null){
                return null;
            }
            long startOffset = offset;
            long endOffset = offset + num - 1;
            int startIndex = (int) (startOffset - seg.getBeg());
            List<ByteBuffer> results = new ArrayList<>(num);
            while (startOffset <= endOffset){
                if (startIndex >= data.size()){
                    seg = queue.nextSegment(seg);
                    if (seg == null) break;
                    data = readData(queue, seg);
                    startIndex = 0;
                }
                results.add(data.get(startIndex ++));
                startOffset ++;
            }
            return results;
        }

        public List<ByteBuffer> readData(Queue queue, final Segment seg){
            Group group = getQueueGroup(queue);
            return caches.computeIfAbsent(new Tuple<>(queue.getId(), seg.getIdx()), k -> seg.getData(group.getDb()));
        }

        public long write(int queueId, ByteBuffer data) throws IOException{
            Queue queue = getQueue(queueId);
            Group group = getQueueGroup(queue);
            int offset = queue.getAndIncrementOffset();

            ByteBuffer wrapper = ByteBuffer.allocate(2 + data.capacity());
            wrapper.putShort((short) data.capacity());
            wrapper.put(data);
            wrapper.flip();

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
            return offset;
        }

    }

}
