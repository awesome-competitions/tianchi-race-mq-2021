package io.openmessaging.impl;

import io.openmessaging.MessageQueue;
import io.openmessaging.model.*;
import io.openmessaging.model.Queue;
import io.openmessaging.utils.ArrayUtils;
import io.openmessaging.utils.BufferUtils;
import io.openmessaging.utils.CollectionUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageQueueImpl extends MessageQueue {

    public static String DATA_ROOT = "D://test/mmap/";              // data root dir.
    public static long DATA_MAPPED_PAGE_SIZE = 1024 * 1024 * 16;    // mmap mapping size of file, unit is KB.
    public static int DATA_CACHED_READER_SIZE = 300;                // reader cached size.
    final static Map<String, Topic> TOPICS = new ConcurrentHashMap<>();

    public MessageQueueImpl(){
        File root = new File(DATA_ROOT);
        if (! root.exists()){
            boolean suc = root.mkdirs();
            if (! suc){
                throw new RuntimeException("create root fail");
            }
        }
        if (! root.isDirectory()){
            throw new RuntimeException("root is not dir");
        }
        Map<String, File> dbs = new HashMap<>();
        Map<String, File> ids = new HashMap<>();
        if (ArrayUtils.isNotEmpty(root.listFiles())){
            for (File file: Objects.requireNonNull(root.listFiles())){
                if (! file.isDirectory()) {
                    if (file.getName().endsWith(".db")){
                        dbs.put(file.getName().substring(0, file.getName().lastIndexOf(".")), file);
                    }else if (file.getName().endsWith(".idx")){
                        ids.put(file.getName().substring(0, file.getName().lastIndexOf(".")), file);
                    }
                }
            }
        }
        for (Map.Entry<String, File> db: dbs.entrySet()){
            File idx = ids.get(db.getKey());
            if (idx != null){
                try {
                    TOPICS.put(db.getKey(), Topic.parse(db.getKey()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
            List<ByteBuffer> results = getTopic(topic).read(queueId, offset, fetchNum);
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

    public static Topic getTopic(String topic){
        return TOPICS.computeIfAbsent(topic, t -> {
            try {
                return new Topic(t);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    public static class Topic{
        private final FileChannel dataChannel;
        private final FileChannel idxChannel;
        private final Map<Integer, Queue> queues;
        private final Lru<Tuple<Integer, Long>, CachedReader> cachedReaders;
        private final AtomicInteger pageOffset;

        public Topic(String topic) throws FileNotFoundException {
            this.queues = new ConcurrentHashMap<>();
            this.cachedReaders = new Lru<>(DATA_CACHED_READER_SIZE, cachedReader -> {
                BufferUtils.clean(cachedReader.getMappedByteBuffer());
            });
            this.dataChannel = new RandomAccessFile(DATA_ROOT + topic + ".db", "rw").getChannel();
            this.idxChannel = new RandomAccessFile(DATA_ROOT + topic + ".idx", "rw").getChannel();
            this.pageOffset = new AtomicInteger();
        }

        public Queue getQueue(int queueId){
            return queues.computeIfAbsent(queueId, Queue::new);
        }

        public List<ByteBuffer> read(int queueId, long offset, int num) throws IOException {
            Queue queue = getQueue(queueId);
            CachedReader cachedReader = getCachedReader(queue, offset);
            if (cachedReader == null){
                return null;
            }
            long startOffset = offset;
            long endOffset = offset + num;
            MappedByteBuffer mappedByteBuffer = cachedReader.getMappedByteBuffer();
            Allocate allocate = cachedReader.getAllocate();
            List<ByteBuffer> results = new ArrayList<>(num);
            byte[] data;
            while(startOffset < endOffset){
                if (allocate.getEnd() < startOffset){
                    allocate = queue.next(allocate);
                    if (allocate == null){
                        break;
                    }
                    BufferUtils.clean(mappedByteBuffer);
                    mappedByteBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, allocate.getPosition(), allocate.getCapacity());
                    continue;
                }
                data = new byte[mappedByteBuffer.getShort()];
                mappedByteBuffer.get(data);
                results.add(ByteBuffer.wrap(data));
                startOffset ++;
            }
            if (allocate != null){
                cachedReader.setAllocate(allocate);
                cachedReader.setMappedByteBuffer(mappedByteBuffer);
                cachedReaders.put(new Tuple<>(queueId, startOffset), cachedReader);
            }
            return results;
        }

        public CachedReader getCachedReader(Queue queue, long offset) throws IOException {
            CachedReader cachedReader = cachedReaders.remove(new Tuple<>(queue.id(), offset));
            if (cachedReader != null){
                return cachedReader;
            }
            Allocate allocate = queue.search(offset);
            if (allocate == null){
                return null;
            }
            MappedByteBuffer mappedByteBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, allocate.getPosition(), allocate.getCapacity());
            long nextOffset = allocate.getStart();
            while(nextOffset < offset){
                nextOffset ++;
                mappedByteBuffer.position(mappedByteBuffer.getShort() + mappedByteBuffer.position());
            }
            return new CachedReader(allocate, mappedByteBuffer);
        }

        public long write(int queueId, ByteBuffer data) throws IOException{
            Queue queue = getQueue(queueId);
            try{
                queue.lock();
                long offset = queue.nextOffset();

                ByteBuffer wrapper = ByteBuffer.allocate(2 + data.capacity());
                wrapper.putShort((short) data.capacity());
                wrapper.put(data);
                wrapper.flip();

                MappedByteBuffer mappedByteBuffer = queue.mappedByteBuffer();
                if (mappedByteBuffer == null || mappedByteBuffer.remaining() < wrapper.capacity()){
                    if (mappedByteBuffer != null) BufferUtils.clean(mappedByteBuffer);
                    Allocate allocate = new Allocate(offset, offset, pageOffset.getAndAdd(1) * DATA_MAPPED_PAGE_SIZE, DATA_MAPPED_PAGE_SIZE);
                    queue.allocate(allocate);
                    mappedByteBuffer = dataChannel.map(FileChannel.MapMode.READ_WRITE, allocate.getPosition(), allocate.getCapacity());
                    queue.mappedByteBuffer(mappedByteBuffer);

                    ByteBuffer idxBuffer = ByteBuffer.allocate(26)
                            .putShort((short) queueId)
                            .putLong(allocate.getStart())
                            .putLong(allocate.getPosition())
                            .putLong(allocate.getCapacity());
                    idxBuffer.flip();
                    idxChannel.write(idxBuffer);
                    idxChannel.force(true);
                }
                queue.lastOfAllocates().setEnd(offset);
                mappedByteBuffer.put(wrapper);
                mappedByteBuffer.force();
                return offset;
            }finally {
                queue.unlock();
            }
        }

        public static Topic parse(String name) throws IOException {
            Topic topic = new Topic(name);
            FileChannel idxChannel = topic.idxChannel;
            ByteBuffer index = ByteBuffer.allocate(26);
            while (idxChannel.read(index) > 0){
                index.flip();
                short queueId = index.getShort();
                long offset = index.getLong();
                long pos = index.getLong();
                long cap = index.getLong();
                topic.getQueue(queueId).allocate(new Allocate(offset, offset, pos, cap));
                index.clear();
            }
            FileChannel dataChannel = topic.dataChannel;
            for (Queue queue: topic.queues.values()){
                Allocate last = queue.lastOfAllocates();
                if (last != null){
                    MappedByteBuffer mappedByteBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, last.getPosition(), last.getCapacity());
                    short size;
                    long endOffset = last.getEnd() - 1;
                    while ((size = mappedByteBuffer.getShort()) > 0){
                        mappedByteBuffer.position(mappedByteBuffer.position() + size);
                        endOffset ++;
                    }
                    last.setEnd(endOffset);
                    queue.mappedByteBuffer(mappedByteBuffer);
                }
            }
            return topic;
        }
    }
}
