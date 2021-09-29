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

public class Mq extends MessageQueue{

    private final Config config;

    private final Map<String, Map<Integer, Queue>> queues;

    private final Barrier barrier;

    private final FileWrapper aof;

    private final Cache cache;

    private static final Logger LOGGER = LoggerFactory.getLogger(Mq.class);

    public Mq(Config config) throws FileNotFoundException {
        this.config = config;
        this.queues = new ConcurrentHashMap<>();
        this.aof = new FileWrapper(new RandomAccessFile(config.getDataDir() + "aof", "rw"));
        this.barrier = new Barrier(config.getMaxCount(), this.aof);
        this.cache = new Cache(config);
        startKiller();
        LOGGER.info("Start");
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

    public Queue getQueue(String topic, int queueId){
        return queues.computeIfAbsent(topic, k ->  new ConcurrentHashMap<>())
                .computeIfAbsent(queueId, k -> {
                    Queue queue = new Queue(cache, aof);
                    queue.setActive(cache.applyActive((int) (Const.K * 17)));
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

        ByteBuffer data = ByteBuffer.allocateDirect(topic.getBytes().length + 4 + buffer.capacity())
                .put(topic.getBytes())
                .putShort((short) queueId)
                .putShort((short) buffer.capacity())
                .put(buffer);
        data.flip();
        buffer.flip();

        long position = barrier.write(data);
        Queue queue = getQueue(topic, queueId);
        queue.write(position - buffer.capacity(), buffer);

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
