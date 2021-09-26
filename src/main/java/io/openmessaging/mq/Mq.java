package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
import io.openmessaging.MessageQueue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Mq {

    private Heap heap;

    private final Config config;

    private final Map<String, Topic> topics;

    private final Barrier barrier;

    private final FileWrapper aof;

    private final FileWrapper tpf;

    public Mq(Config config) throws FileNotFoundException {
        this.config = config;
        this.topics = new ConcurrentHashMap<>();
        this.aof = new FileWrapper(new RandomAccessFile(config.getDataDir() + "aof", "rw"));
        this.tpf = new FileWrapper(new RandomAccessFile(config.getDataDir() + "tpf", "rw"));
        this.barrier = new Barrier(config.getMaxCount(), this.aof);
        if (config.getHeapDir() != null){
            this.heap = Heap.exists(config.getHeapDir()) ? Heap.openHeap(config.getHeapDir()) : Heap.createHeap(config.getHeapDir(), config.getHeapSize());
        }
    }

    Topic getTopic(String name){
       return topics.computeIfAbsent(name, k -> new Topic());
    }

    Data applyBlock( ByteBuffer buffer){
        AnyMemoryBlock block = heap.allocateMemoryBlock(buffer.capacity());
        byte[] bytes = new byte[buffer.capacity()];
        buffer.get(bytes);
        block.copyFromArray(bytes, 0, 0, bytes.length);
        return new PMem(block);
    }

    Data applyData(ByteBuffer buffer){
        return heap == null ? new Dram(buffer) : applyBlock(buffer);
    }

    void clear(Topic topic) throws IOException {
        long count = 0;
        long capacity = 0;
        boolean finished = false;
        Map<Integer, List<Data>> outs = new HashMap<>();
        for (Map.Entry<Integer, Queue> entry: topic.getQueues().entrySet()){
            List<Data> list = outs.computeIfAbsent(entry.getKey(), k->new ArrayList<>());
            Queue queue = entry.getValue();
            Iterator<Data> iter = queue.getRecords().iterator();
            while (iter.hasNext() && ! finished){
                Data record = iter.next();
                capacity += record.size();
                count ++;
                list.add(record);
                iter.remove();
                if (capacity >= config.getTopicShrinkSize()){
                    finished = true;
                }
            }
            if (finished){
                break;
            }
        }
        topic.increment(capacity);
        for (Map.Entry<Integer, List<Data>> out: outs.entrySet()){
            Queue queue = topic.getQueue(out.getKey());
            List<Data> list = out.getValue();
            if (list.size() > 0){
                capacity = 0;
                count = list.size();
                long startOffset = queue.getStartOffset();
                long endOffset = startOffset + count - 1;
                List<ByteBuffer> buffers = new ArrayList<>(list.size());
                for (Data data: list){
                    capacity += data.size();
                    buffers.add(data.get());
                    data.clear();
                }
                long position = this.tpf.write(buffers.toArray(Barrier.EMPTY));
                Block block = new Block(startOffset, endOffset, position, capacity);
                queue.getBlocks().add(block);
            }
        }

    }

    long append(Topic topic, Queue queue, Data data) throws IOException {
        long offset = queue.append(data);
        topic.increment(data.size());
        topic.refresh(queue);
        if (topic.getSize() > config.getTopicMaxSize()){
            clear(topic);
        }
        return offset;
    }

    public long append(String name, int queueId, ByteBuffer buffer) throws IOException {
        Topic topic = getTopic(name);
        Queue queue = topic.getQueue(queueId);
        long offset = append(topic, queue, applyData(buffer));

        barrier.write(buffer);
        barrier.await(10, TimeUnit.SECONDS);
        return offset;
    }

    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return null;
    }



}
