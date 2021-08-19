package io.openmessaging;

import sun.nio.ch.FileChannelImpl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SSDMessageQueueImpl extends MessageQueue{

    static final Map<String, Topic> topics = new ConcurrentHashMap<>();
    static final int poolsMaxSize = 10;
    static final long pageSize = 1024 * 1024 * 4;    // 4M
    static final String root = "D://test/";

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
        return null;
    }

    public static Topic getTopic(String topic) throws FileNotFoundException {
        return topics.getOrDefault(topic, new Topic(topic));
    }

    public static class Reader{
        private Tuple<Long, MappedByteBuffer> tuple;
    }

    public static class Topic{
        private final String topic;
        private RandomAccessFile data;
        private final RandomAccessFile idx;

        private final Map<Integer, List<Allocate>> indexes;
        private final Map<Integer, MappedByteBuffer> writeMappedByteBuffers;

        private final FileChannel dataChannel;
        private final FileChannel idxChannel;
        private final Map<Integer, AtomicLong> offsets;
        private int pageOffset;

        public Topic(String topic) throws FileNotFoundException {
            this.topic = topic;
            this.data = new RandomAccessFile(root + topic + ".db", "rw");
            this.idx = new RandomAccessFile(root + topic + ".idx", "rw");
            this.dataChannel = data.getChannel();
            this.idxChannel = idx.getChannel();
            this.offsets = new ConcurrentHashMap<>();
            this.indexes= new HashMap<>();
            this.writeMappedByteBuffers = new HashMap<>();
        }

        public ByteBuffer[] read(int queueId, long offset, int num) throws IOException {
            List<Allocate> allocates = indexes.get(queueId);
            if (CollectionUtils.isEmpty(allocates)){
                return null;
            }
            Allocate allocate = CollectionUtils.lastOf(allocates);
            for (int i = 0; i < allocates.size(); i ++){
                if (allocates.get(i).getOffset() > offset){
                    // i is always greater than 0
                    allocate = allocates.get(i - 1);
                    break;
                }
            }
            MappedByteBuffer mappedByteBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, allocate.getPosition(), allocate.getCapacity());
            //read
            return null;
        }

        public long write(int queueId, ByteBuffer data) throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
            long offset = offsets.computeIfAbsent(queueId, q -> new AtomicLong()).getAndAdd(1);
            ByteBuffer wrapper = ByteBuffer.allocate(2 + data.capacity());
            wrapper.putShort((short) data.capacity());
            wrapper.put(data);
            wrapper.flip();

            MappedByteBuffer mappedByteBuffer = writeMappedByteBuffers.get(queueId);
            if (mappedByteBuffer == null || mappedByteBuffer.position() + wrapper.capacity() > mappedByteBuffer.capacity()){
                List<Allocate> allocates = indexes.computeIfAbsent(queueId, ArrayList::new);
                Allocate allocate = new Allocate(offset, pageOffset * pageSize, pageSize);
                allocates.add(new Allocate(offset, pageOffset * pageSize, pageSize));

                BufferUtils.unmap(mappedByteBuffer);
                mappedByteBuffer = dataChannel.map(FileChannel.MapMode.READ_WRITE, allocate.getPosition(), allocate.getCapacity());

                ByteBuffer idxBuffer = ByteBuffer.allocate(14)
                                        .putShort((short) queueId)
                                        .putLong(allocate.getOffset())
                                        .putLong(allocate.getPosition())
                                        .putLong(allocate.getCapacity());
                idxBuffer.flip();
                idxChannel.write(idxBuffer);
            }
            mappedByteBuffer.put(wrapper);
            return offset;
        }
    }
}
