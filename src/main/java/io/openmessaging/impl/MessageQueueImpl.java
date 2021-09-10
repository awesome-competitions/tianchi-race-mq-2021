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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageQueueImpl extends MessageQueue {

//    public static String DATA_ROOT = "D://test/mmap/";              // data root dir.
    public static String DATA_ROOT = "/essd/";              // data root dir.
    public static int DATA_MAPPED_PAGE_SIZE = 1024 * 1024 * 16;    // mmap mapping size of file, unit is KB.
    public static int DATA_CACHED_PAGE_SIZE = 1024 * 1024 * 1024 / DATA_MAPPED_PAGE_SIZE;       // reader cached size.
    public static final ExecutorService TPE = Executors.newFixedThreadPool(50);

    private Map<String, Topic> topics = new ConcurrentHashMap<>();

    public MessageQueueImpl(){}

    public void cleanDB(){
        File root = new File(DATA_ROOT);
        if (root.exists() && root.isDirectory()){
            if (ArrayUtils.isNotEmpty(root.listFiles())){
                for (File file: Objects.requireNonNull(root.listFiles())){
                    if (file.exists() && ! file.isDirectory()){
                        boolean deleted = file.delete();
                    }
                }
            }
        }
    }

    public void loadDB(){
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
                    topics.put(db.getKey(), Topic.parse(db.getKey()));
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

    public Topic getTopic(String topic){
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
        private final Lru<Tuple<Integer, Integer>, FutureTask<List<ByteBuffer>>> cachedPages;
        private final AtomicInteger pageOffset;

        public Topic(String topic) throws FileNotFoundException {
            this.queues = new ConcurrentHashMap<>();
            this.cachedPages = new Lru<>(DATA_CACHED_PAGE_SIZE, cachedReaders -> {});
            this.dataChannel = new RandomAccessFile(DATA_ROOT + topic + ".db", "rw").getChannel();
            this.idxChannel = new RandomAccessFile(DATA_ROOT + topic + ".idx", "rw").getChannel();
            this.pageOffset = new AtomicInteger();
        }

        public Queue getQueue(int queueId){
            return queues.computeIfAbsent(queueId, Queue::new);
        }

        public List<ByteBuffer> getRecords(Queue queue, final Allocate allocate){
            return getRecords(queue, allocate, true);
        }

        public List<ByteBuffer> getRecords(Queue queue, final Allocate allocate, boolean load){
            try {
                FutureTask<List<ByteBuffer>> futureTask = getFutureRecords(queue.id(), allocate, load);
                if (futureTask == null){
                    return null;
                }
                return futureTask.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            } finally {
                if (load){
                    Allocate nextAllocate = queue.next(allocate);
                    if (nextAllocate != null){
                        getFutureRecords(queue.id(), nextAllocate, true);
                    }
                }
            }
            return null;
        }

        public FutureTask<List<ByteBuffer>> getFutureRecords(int queueId, final Allocate allocate, boolean load){
            if (! load){
                return cachedPages.get(new Tuple<>(queueId, allocate.getIndex()));
            }
            return cachedPages.computeIfAbsent(new Tuple<>(queueId, allocate.getIndex()), k -> {
                FutureTask<List<ByteBuffer>> futureTask = new FutureTask<>(()-> allocate.load(dataChannel));
                TPE.execute(futureTask);
                return futureTask;
            });
        }

        public List<ByteBuffer> read(int queueId, long offset, int num) throws IOException {
            Queue queue = getQueue(queueId);
            Allocate allocate = queue.search(offset);
            if (allocate == null){
                return null;
            }
            List<ByteBuffer> records = getRecords(queue, allocate);
            if (records == null){
                return null;
            }
            long startOffset = offset;
            long endOffset = offset + num - 1;
            int startIndex = (int) (startOffset - allocate.getStart());
            List<ByteBuffer> results = new ArrayList<>(num);
            while (startOffset <= endOffset){
                if (startIndex >= records.size()){
                    allocate = queue.next(allocate);
                    if (allocate == null) break;
                    records = getRecords(queue, allocate);
                    startIndex = 0;
                }
                results.add(records.get(startIndex ++));
                startOffset ++;
            }
            return results;
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

                Allocate allocate = queue.lastOfAllocates();
                MappedByteBuffer mappedByteBuffer = queue.mappedByteBuffer();
                if (mappedByteBuffer == null || mappedByteBuffer.remaining() < wrapper.capacity()){
                    if (mappedByteBuffer != null) BufferUtils.clean(mappedByteBuffer);
                    allocate = new Allocate(offset, offset, (long) pageOffset.getAndAdd(1) * DATA_MAPPED_PAGE_SIZE, DATA_MAPPED_PAGE_SIZE);
                    queue.lastRecords().clear();
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
                List<ByteBuffer> records = getRecords(queue, allocate, false);
                if (records != null){
                    records.add(data);
                }
                allocate.setEnd(offset);
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
                    MappedByteBuffer mappedByteBuffer = dataChannel.map(FileChannel.MapMode.READ_WRITE, last.getPosition(), last.getCapacity());
                    short size;
                    long endOffset = last.getEnd() - 1;
                    while ((size = mappedByteBuffer.getShort()) > 0){
                        mappedByteBuffer.position(mappedByteBuffer.position() + size);
                        endOffset ++;
                    }
                    mappedByteBuffer.position(mappedByteBuffer.position() - 2);
                    last.setEnd(endOffset);
                    queue.resetOffset(endOffset + 1);
                    queue.mappedByteBuffer(mappedByteBuffer);
                }
            }
            return topic;
        }
    }
}
