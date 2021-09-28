package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
import io.openmessaging.MessageQueue;
import io.openmessaging.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class Mq extends MessageQueue{

    private Heap heap;

    private final Config config;

    private final Map<String, Map<Integer, Queue>> queues;

    private final Barrier barrier;

    private final FileWrapper aof;

    private final FileWrapper tpf;

    private final LinkedBlockingQueue<AnyMemoryBlock> blocks = new LinkedBlockingQueue<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(Mq.class);

    public Mq(Config config) throws FileNotFoundException {
        this.config = config;
        this.queues = new ConcurrentHashMap<>();
        this.aof = new FileWrapper(new RandomAccessFile(config.getDataDir() + "aof", "rw"));
        this.tpf = new FileWrapper(new RandomAccessFile(config.getDataDir() + "tpf", "rw"));
        this.barrier = new Barrier(config.getMaxCount(), this.aof);
        if (config.getHeapDir() != null){
            this.heap = Heap.exists(config.getHeapDir()) ? Heap.openHeap(config.getHeapDir()) : Heap.createHeap(config.getHeapDir(), config.getHeapSize());
        }
        startKiller();
        startProducer();
    }

    void startKiller(){
        new Thread(()->{
            try {
                if (config.getLiveTime() > 0) {
                    Thread.sleep(config.getLiveTime());
                    System.exit(-1);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    void startProducer(){
        Thread monitor = new Thread(()->{
            for (int i = 0; i < 18 * 10000; i ++){
                blocks.add(heap.allocateCompactMemoryBlock(config.getActiveSize()));
            }
        });
        monitor.setDaemon(true);
        monitor.start();
    }

    Data apply(int capacity){
        if (heap == null){
            return new Dram(capacity);
        }
        AnyMemoryBlock block = blocks.poll();
        if (block == null){
            block = heap.allocateCompactMemoryBlock(capacity);
        }
        return new PMem(block, capacity);
    }

    public Queue getQueue(String topic, int queueId){
        return queues.computeIfAbsent(topic, k ->  new ConcurrentHashMap<>())
                .computeIfAbsent(queueId, k -> {
                    Queue queue = new Queue();
                    queue.setActive(apply(config.getActiveSize()));
                    Monitor.queueCount ++;
                    return queue;
                });
    }


    public long append(String topic, int queueId, ByteBuffer buffer)  {
        Monitor.appendCount ++;
        Monitor.appendSize += buffer.capacity();
        if (Monitor.appendCount % 100000 == 0){
            LOGGER.info(Monitor.information());
        }
        Queue queue = getQueue(topic, queueId);
        queue.write(tpf, buffer);
        buffer.flip();

        ByteBuffer header = ByteBuffer.allocateDirect(topic.getBytes().length + 4)
                .put(topic.getBytes())
                .putShort((short) queueId)
                .putShort((short) buffer.capacity());
        header.flip();
        barrier.write(header, buffer);
        barrier.await(30, TimeUnit.SECONDS);
        return queue.getOffset();
    }

    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        Queue queue = getQueue(topic, queueId);
        List<ByteBuffer> buffers = queue.read(offset, fetchNum);
        Map<Integer, ByteBuffer> results = new HashMap<>();
        if (CollectionUtils.isEmpty(buffers)){
            return results;
        }
        for (int i = 0; i < buffers.size(); i ++){
            results.put(i, buffers.get(i));
        }
        return results;
    }

}
