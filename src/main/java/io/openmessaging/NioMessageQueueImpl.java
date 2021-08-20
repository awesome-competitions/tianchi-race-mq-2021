package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class NioMessageQueueImpl extends MessageQueue{

    static final Map<String, Topic> topics = new ConcurrentHashMap<>();
    static final long pageSize = 1024 * 1024 * 64;    // 4M
    static final String root = "D://test/nio/";

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
        private final Map<Integer, Map<Long, Tuple<Integer, MappedByteBuffer>>> readMappedByteBuffer;

        private final Map<Integer, Allocate> cachedWriteAllocates;
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
            this.cachedWriteAllocates = new ConcurrentHashMap<>();
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
            Allocate allocate = allocates.get(index);
            long startOffset = offset;
            List<ByteBuffer> results = new ArrayList<>(num);
            while(num > 0){
                if (allocate.getEnd() > 0 && allocate.getEnd() < startOffset){
                    if (index >= allocates.size() - 1){
                        break;
                    }
                    BufferUtils.clean(mappedByteBuffer);
                    allocate = allocates.get(++index);
                    mappedByteBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, allocate.getPosition(), allocate.getCapacity());
                    continue;
                }
                byte[] data = new byte[mappedByteBuffer.getShort()];
                mappedByteBuffer.get(data);
                results.add(ByteBuffer.wrap(data));
                num --;
                startOffset ++;
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
                if (allocates.get(i).getStart() > offset){
                    // i is always greater than 0
                    allocate = allocates.get(i - 1);
                    allocate.setEnd(allocates.get(i).getStart() - 1);
                    index = i - 1;
                    break;
                }
            }
            MappedByteBuffer mappedByteBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, allocate.getPosition(), allocate.getCapacity());
            long nextOffset = allocate.getStart();
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

            Allocate allocate = cachedWriteAllocates.get(queueId);
            if (allocate == null || allocate.getPosition() + wrapper.capacity() > allocate.getCapacity()){
                List<Allocate> allocates = indexes.computeIfAbsent(queueId, ArrayList::new);
                allocate = new Allocate(offset, 0, pageOffset * pageSize, pageSize);
                if (allocates.size() > 0){
                    CollectionUtils.lastOf(allocates).setEnd(offset - 1);
                }
                cachedWriteAllocates.put(queueId, allocate);
                allocates.add(allocate);
                pageOffset ++;
                ByteBuffer idxBuffer = ByteBuffer.allocate(26)
                                        .putShort((short) queueId)
                                        .putLong(allocate.getEnd())
                                        .putLong(allocate.getPosition())
                                        .putLong(allocate.getCapacity());
                idxBuffer.flip();
                idxChannel.write(idxBuffer);
                idxChannel.force(true);
            }

            dataChannel.position(allocate.getPosition());
            dataChannel.write(wrapper);
            dataChannel.force(true);
            allocate.setPosition(allocate.getPosition() + wrapper.capacity());
            return offset;
        }
    }
}
