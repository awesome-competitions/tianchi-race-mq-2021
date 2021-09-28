package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
import io.openmessaging.MessageQueue;
import io.openmessaging.consts.Const;
import io.openmessaging.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Mq extends MessageQueue{

    private Heap heap;

    private final Config config;

    private final Map<String, Map<Integer, Queue>> queues;

    private final Barrier barrier;

    private final FileWrapper aof;

    private final ThreadLocal<FileWrapper> tpf;

    private static final Logger LOGGER = LoggerFactory.getLogger(Mq.class);

    private static final ThreadPoolExecutor POOLS = (ThreadPoolExecutor) Executors.newFixedThreadPool(200);

    public Mq(Config config) throws FileNotFoundException {
        this.config = config;
        this.queues = new ConcurrentHashMap<>();
        this.aof = new FileWrapper(new RandomAccessFile(config.getDataDir() + "aof", "rw"));
        this.tpf = new ThreadLocal<>();
        this.barrier = new Barrier(config.getMaxCount(), this.aof);
        if (config.getHeapDir() != null){
            this.heap = Heap.exists(config.getHeapDir()) ? Heap.openHeap(config.getHeapDir()) : Heap.createHeap(config.getHeapDir(), config.getHeapSize());
        }
        if (config.getLiveTime() > 0){
            startKiller();
        }
        startMonitor();
    }

    void startKiller(){
        new Thread(()->{
            try {
                Thread.sleep(config.getLiveTime());
                System.exit(-1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    void startMonitor(){
        Thread monitor = new Thread(()->{
        });
        monitor.setDaemon(true);
        monitor.start();
    }

    FileWrapper tpf(){
        if (tpf.get() == null){
            try {
                tpf.set(new FileWrapper(new RandomAccessFile(config.getDataDir() + "tpf_" + Thread.currentThread().getId(), "rw")));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        return tpf.get();
    }

    Data apply(int capacity){
        if (heap == null){
            return new Dram(capacity);
        }
        return new PMem(POOLS.submit(() ->  heap.allocateCompactMemoryBlock(capacity)), capacity);
    }

    public Queue getQueue(String topic, int queueId){
        return queues.computeIfAbsent(topic, k ->  new ConcurrentHashMap<>())
                .computeIfAbsent(queueId, k -> {
                    Queue queue = new Queue();
                    queue.setActive(apply(config.getActiveSize()));
                    queueCount ++;
                    return queue;
                });
    }


    long size = 0;
    int count = 0;
    int queueCount = 0;
    public long append(String topic, int queueId, ByteBuffer buffer)  {
        ++count;
        size += buffer.capacity();
        if (count % 100000 == 0){
            LOGGER.info("append count {}, size {}, queueCount {}", count, size, queueCount);
        }
        Queue queue = getQueue(topic, queueId);
        long offset = queue.write(tpf(), buffer);
        buffer.flip();

        ByteBuffer header = ByteBuffer.allocateDirect(topic.getBytes().length + 4)
                .put(topic.getBytes())
                .putShort((short) queueId)
                .putShort((short) buffer.capacity());
        header.flip();
        barrier.write(header, buffer);
        barrier.await(30, TimeUnit.SECONDS);
        return offset;
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
