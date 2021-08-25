package io.openmessaging.impl;

import io.openmessaging.MessageQueue;
import io.openmessaging.model.*;
import io.openmessaging.model.Queue;
import io.openmessaging.utils.BufferUtils;
import io.openmessaging.utils.CollectionUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class MessageQueueImpl extends MessageQueue {

    static final Map<String, Topic> topics = new ConcurrentHashMap<>();
    static final long pageSize = 1024 * 1024 * 16;    // 4M
    static final int cachedReaderSize = 600;
    static final String root = "D://test/mmap/";

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        try {
            Topic t = getTopic(topic);
            return t.write(queueId, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        try {
            Topic t = getTopic(topic);
            List<ByteBuffer> results = t.read(queueId, offset, fetchNum);
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
        return topics.computeIfAbsent(topic, t -> {
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
            this.cachedReaders = new Lru<>(cachedReaderSize);
            this.dataChannel = new RandomAccessFile(root + topic + ".db", "rw").getChannel();
            this.idxChannel = new RandomAccessFile(root + topic + ".idx", "rw").getChannel();
            this.pageOffset = new AtomicInteger(0);
        }

        public List<ByteBuffer> read(int queueId, long offset, int num) throws IOException {
            Queue queue = queues.computeIfAbsent(queueId, Queue::new);
            CachedReader cachedReader = getCachedReader(queue, offset);
            if (cachedReader == null){
                return null;
            }

            long startOffset = offset;
            long endOffset = offset + num;
            MappedByteBuffer mappedByteBuffer = cachedReader.getMappedByteBuffer();
            Allocate allocate = cachedReader.getAllocate();
            List<ByteBuffer> results = new ArrayList<>(num);
            byte[] data = null;
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
            cachedReader.setAllocate(allocate);
            cachedReader.setMappedByteBuffer(mappedByteBuffer);
            cachedReaders.put(new Tuple<>(queueId, startOffset), cachedReader);
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
            Queue queue = queues.computeIfAbsent(queueId, Queue::new);
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
                    Allocate allocate = new Allocate(offset, 0, pageOffset.getAndAdd(1) * pageSize, pageSize);
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
                return offset;
            }finally {
                queue.unlock();
            }
        }
    }
}
