package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
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

    public static class Topic{
        private final String topic;
        private RandomAccessFile data;
        private final RandomAccessFile idx;
        private final Map<Integer, List<Tuple<Long, MappedByteBuffer>>> indexes;
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
        }

        public long read(int queueId, long offset){
            
        }

        public long write(int queueId, ByteBuffer data) throws IOException {
            long offset = offsets.computeIfAbsent(queueId, q -> new AtomicLong()).getAndAdd(1);
            List<Tuple<Long, MappedByteBuffer>> tuples = indexes.computeIfAbsent(queueId, ArrayList::new);
            Tuple<Long, MappedByteBuffer> tuple = tuples.isEmpty() ? null : tuples.get(tuples.size() - 1);

            ByteBuffer wrapper = ByteBuffer.allocate(2 + data.capacity());
            wrapper.putShort((short) data.capacity());
            wrapper.put(data);
            wrapper.flip();
            boolean expanded = false;
            if (tuple == null || tuple.getV().position() + wrapper.capacity() > tuple.getV().capacity()){
                tuple = new Tuple<>(offset, dataChannel.map(FileChannel.MapMode.READ_WRITE, pageOffset * pageSize, pageSize));
                tuples.add(tuple);
                expanded = true;
            }
            if (expanded){
                ByteBuffer idxBuffer = ByteBuffer.allocate(14);
                idxBuffer.putShort((short) queueId)
                        .putLong(offset)
                        .putInt(pageOffset ++)
                        .flip();
                idxChannel.write(idxBuffer);
            }
            MappedByteBuffer mappedByteBuffer = tuple.getV();
            mappedByteBuffer.put(wrapper);
            System.out.println(mappedByteBuffer.toString());
            return offset;
        }
    }
}
