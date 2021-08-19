package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
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
        private final String topic;
        private RandomAccessFile data;
        private final RandomAccessFile idx;

        private final Map<Integer, List<Allocate>> indexes;
        private final Map<Integer, MappedByteBuffer> writeMappedByteBuffers;
        private final Map<Integer, Map<Long, Tuple<Integer, MappedByteBuffer>>> readMappedByteBuffer;

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
            this.indexes= new ConcurrentHashMap<>();
            this.writeMappedByteBuffers = new ConcurrentHashMap<>();
            this.readMappedByteBuffer = new ConcurrentHashMap<>();
        }

        public List<ByteBuffer> read(int queueId, long offset, int num) throws IOException {
            Tuple<Integer, MappedByteBuffer> tuple = getReadMappedByteBuffer(queueId, offset);
            if (tuple == null){
                return null;
            }
            int index = tuple.getK();
            MappedByteBuffer mappedByteBuffer = tuple.getV();
            List<Allocate> allocates = indexes.get(queueId);
            List<ByteBuffer> results = new ArrayList<>(num);
            while(num > 0){
                short size;
                if (mappedByteBuffer.capacity() - mappedByteBuffer.position() < 2 || (size = mappedByteBuffer.getShort()) == 0){
                    if (index >= allocates.size() - 1){
                        break;
                    }
                    BufferUtils.clean(mappedByteBuffer);
                    Allocate allocate = allocates.get(index ++);
                    mappedByteBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, allocate.getPosition(), allocate.getCapacity());
                    continue;
                }
                num --;
                byte[] data = new byte[size];
                mappedByteBuffer.get(data);
                results.add(ByteBuffer.wrap(data));
            }
            tuple.setK(index);
            tuple.setV(mappedByteBuffer);
            readMappedByteBuffer.get(queueId).put(offset + results.size() - num, tuple);
            return results;
        }

        public Tuple<Integer, MappedByteBuffer> getReadMappedByteBuffer(int queueId, long offset) throws IOException {
            Map<Long, Tuple<Integer, MappedByteBuffer>> mappedByteBuffers = readMappedByteBuffer.computeIfAbsent(queueId, ConcurrentHashMap::new);
            Tuple<Integer, MappedByteBuffer> tuple = mappedByteBuffers.get(offset);
            if (tuple != null){
                return mappedByteBuffers.remove(offset);
            }
            List<Allocate> allocates = indexes.get(queueId);
            if (CollectionUtils.isEmpty(allocates)){
                return null;
            }
            Allocate allocate = CollectionUtils.lastOf(allocates);
            int index = allocates.size() - 1;
            for (int i = 0; i < allocates.size(); i ++){
                if (allocates.get(i).getOffset() > offset){
                    // i is always greater than 0
                    allocate = allocates.get(i - 1);
                    index = i - 1;
                    break;
                }
            }
            MappedByteBuffer mappedByteBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, allocate.getPosition(), allocate.getCapacity());
            long nextOffset = allocate.getOffset();
            while(true){
                if (nextOffset < offset){
                    nextOffset ++;
                    mappedByteBuffer.position(mappedByteBuffer.getShort() + mappedByteBuffer.position());
                    continue;
                }
                break;
            }
            return new Tuple<>(index, mappedByteBuffer);
        }

        public long write(int queueId, ByteBuffer data) throws IOException{
            AtomicLong atomicLong = offsets.computeIfAbsent(queueId, q -> new AtomicLong());
            long offset = atomicLong.getAndAdd(1);
            ByteBuffer wrapper = ByteBuffer.allocate(2 + data.capacity());
            wrapper.putShort((short) data.capacity());
            wrapper.put(data);
            wrapper.flip();

            MappedByteBuffer mappedByteBuffer = writeMappedByteBuffers.get(queueId);
            boolean missingMappedByteBuffer = mappedByteBuffer == null;
            if (missingMappedByteBuffer || mappedByteBuffer.position() + wrapper.capacity() > mappedByteBuffer.capacity()){
                List<Allocate> allocates = indexes.computeIfAbsent(queueId, ArrayList::new);
                Allocate allocate = new Allocate(offset, pageOffset * pageSize, pageSize);
                allocates.add(allocate);
                pageOffset ++;

                if (! missingMappedByteBuffer) BufferUtils.clean(mappedByteBuffer);
                mappedByteBuffer = dataChannel.map(FileChannel.MapMode.READ_WRITE, allocate.getPosition(), allocate.getCapacity());
                writeMappedByteBuffers.put(queueId, mappedByteBuffer);

                ByteBuffer idxBuffer = ByteBuffer.allocate(26)
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
