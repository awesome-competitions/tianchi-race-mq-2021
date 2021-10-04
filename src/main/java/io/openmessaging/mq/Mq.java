package io.openmessaging.mq;

import io.openmessaging.MessageQueue;
import io.openmessaging.consts.Const;
import io.openmessaging.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class Mq extends MessageQueue{

    private final Config config;

    private final Map<Integer, Topic> topics;

    private final Barrier barrier;

    private final FileWrapper aof;

    private final Cache cache;

    private static final Logger LOGGER = LoggerFactory.getLogger(Mq.class);

    private static final Map<String, Integer> TID = new ConcurrentHashMap<>();

    private static final ThreadLocal<ByteBuffer> BUFFERS = new ThreadLocal<>();

    private static final ThreadLocal<Barrier> BARRIERS = new ThreadLocal<>();

    private static final LinkedBlockingQueue<Barrier> POOLS = new LinkedBlockingQueue<>();

    public Mq(Config config) throws FileNotFoundException {
        LOGGER.info("Mq init");
        this.config = config;
        this.topics = new ConcurrentHashMap<>();
        this.aof = new FileWrapper(new RandomAccessFile(config.getDataDir() + "aof", "rw"));
        this.barrier = new Barrier(config.getMaxCount(), this.aof);
        this.cache = new Cache(config.getHeapDir(), config.getHeapSize());
        initPools();
        startKiller();
        LOGGER.info("Mq completed");
    }

    void initPools(){
        for (int i = 0; i < 2; i ++){
            Barrier barrier = new Barrier(20, aof);
            for (int j = 0; j < 20; j ++){
                POOLS.add(barrier);
            }
        }
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

    public Queue getQueue(int topic, int queueId){
        return topics.computeIfAbsent(topic, k -> {
            try {
                return new Topic(new FileWrapper(new RandomAccessFile(config.getDataDir() + "tpl_" + topic, "rw")), config.getPageSize());
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }).getQueue(queueId, cache);
    }

    public ByteBuffer getByteBuffer(){
        ByteBuffer buffer = BUFFERS.get();
        if (buffer == null){
            buffer = ByteBuffer.allocateDirect((int) (Const.K * 17 + 10));
            BUFFERS.set(buffer);
        }
        return buffer;
    }

    public Barrier getBarrier(){
        Barrier barrier = BARRIERS.get();
        if (barrier == null){
            barrier = POOLS.poll();
            BARRIERS.set(barrier);
        }
        return barrier;
    }

    public Config getConfig() {
        return config;
    }

    public long append(String topic, int queueId, ByteBuffer buffer)  {
        return append(TID.computeIfAbsent(topic, k -> Integer.parseInt(k.substring(5))), queueId, buffer);
    }

    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return getRange(TID.computeIfAbsent(topic, k -> Integer.parseInt(k.substring(5))), queueId, offset, fetchNum);
    }

    public long append(int topic, int queueId, ByteBuffer buffer)  {
        Monitor.appendCount ++;
        Monitor.appendSize += buffer.limit();
        if (Monitor.appendCount % 100000 == 0){
            LOGGER.info(Monitor.information());
        }

        Queue queue = getQueue(topic, queueId);
        long offset = queue.nextOffset();

        ByteBuffer data = getByteBuffer()
                .put((byte) topic)
                .putShort((short) queueId)
                .putInt((int) offset)
                .putShort((short) buffer.limit())
                .put(buffer);
        data.flip();
        buffer.flip();

        getBarrier().write(data);
//        queue.write(buffer);

        getBarrier().await(30, TimeUnit.SECONDS);
        return queue.getOffset();
    }

    public Map<Integer, ByteBuffer> getRange(int topic, int queueId, long offset, int fetchNum) {
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
