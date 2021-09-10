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

    private String dataDir;

    private int pageSize;

    private int cacheSize;

    private Map<String, Topic> topics;

    private static final Logger log = LoggerFactory.getLogger(MessageQueueImpl.class);

    public MessageQueueImpl() {
//        this("/essd/", 1024 * 1024 * 16, 10);
        this("D:\\test\\nio\\", 1024 * 1024 * 16, 10);
    }

    public MessageQueueImpl(String dataDir, int pageSize, int cacheSize) {
        this.dataDir = dataDir;
        this.pageSize = pageSize;
        this.cacheSize = cacheSize;
        this.topics = new ConcurrentHashMap<>();
    }

    public void loadDB(){}

    public void cleanDB(){
        File root = new File(dataDir);
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
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        try {
            Topic topic1 = getTopic(topic);
            List<ByteBuffer> results = topic1.read(queueId, offset, fetchNum);
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
            topic = new Topic(name, dataDir, pageSize, cacheSize);
            topics.put(name, topic);
        }
        return topic;
    }

    public static class Topic{
        private final String name;
        private final FileWrapper db;
        private final FileWrapper idx;
        private final Map<Integer, Queue> queues;
        private final Lru<Tuple<Integer, Integer>, List<ByteBuffer>> caches;
        private final AtomicInteger pageOffset;
        private final int pageSize;

        public Topic(String name, String dataDir, int pageSize, int cacheSize) throws FileNotFoundException {
            this.queues = new ConcurrentHashMap<>();
            this.db = new FileWrapper(new RandomAccessFile(dataDir + name + ".db", "rw"));
            this.idx = new FileWrapper(new RandomAccessFile(dataDir + name + ".idx", "rw"));
            this.pageSize = pageSize;
            this.pageOffset = new AtomicInteger();
            this.caches = new Lru<>(cacheSize, List::clear);
            this.name = name;
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
            return caches.computeIfAbsent(new Tuple<>(queue.getId(), seg.getIdx()), k -> seg.getData(db));
        }

        public long write(int queueId, ByteBuffer data) throws IOException{
            Queue queue = getQueue(queueId);
            int offset = queue.getAndIncrementOffset();

            ByteBuffer wrapper = ByteBuffer.allocate(2 + data.capacity());
            wrapper.putShort((short) data.capacity());
            wrapper.put(data);
            wrapper.flip();

            Segment last = queue.getLast();
            if (last == null || ! last.writable(wrapper.capacity())){
                last = new Segment(offset, offset, (long) pageOffset.getAndAdd(1) * pageSize, pageSize);
                queue.addSegment(last);
                ByteBuffer idxBuffer = ByteBuffer.allocate(26)
                        .putShort((short) queueId)
                        .putLong(last.getBeg())
                        .putLong(last.getPos())
                        .putLong(last.getCap());
                idxBuffer.flip();
                idx.write(idxBuffer);
            }
            last.setEnd(offset);
            last.write(db, wrapper);
            return offset;
        }

    }
}
