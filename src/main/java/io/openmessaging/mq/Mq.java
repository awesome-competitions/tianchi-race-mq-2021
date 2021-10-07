package io.openmessaging.mq;

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

    private final Map<Integer, Map<Integer, Queue>> queues;

    private final FileWrapper aof;

    private final Cache cache;

    private final Barrier barrier;

    private static final Logger LOGGER = LoggerFactory.getLogger(Mq.class);

    private final Map<String, Integer> TID = new ConcurrentHashMap<>();

    private final ThreadLocal<ByteBuffer> BUFFERS = new ThreadLocal<>();

    public Mq(Config config) throws IOException {
        LOGGER.info("Mq init");
        this.config = config;
        this.queues = new ConcurrentHashMap<>();
        this.aof = new FileWrapper(new RandomAccessFile(config.getDataDir() + "aof", "rw"));
        this.barrier = new Barrier(config.getMaxCount(), aof);
        this.cache = new Cache(config.getHeapDir(), config.getHeapSize());
        loadAof();
        startKiller();
        LOGGER.info("Mq completed");
    }

    void loadAof() throws IOException {
        long position = 0;
        ByteBuffer header = ByteBuffer.allocate(5);
        while(true){
            aof.read(position, header);
            position += 5;
            header.flip();
            if (header.remaining() < 5){
                break;
            }
            int topic = header.get();
            int queueId = header.getShort();
            int size = header.getShort();
            header.clear();
            if (size == 0){
                break;
            }
            ByteBuffer data = ByteBuffer.allocate(size);
            aof.read(position, data);
            data.flip();
            _append(topic, queueId, position, data);
            position += size;
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
        return queues.computeIfAbsent(topic, k -> new HashMap<>()).computeIfAbsent(queueId, k -> new Queue(aof, cache));
    }

    public ByteBuffer getByteBuffer(){
        ByteBuffer buffer = BUFFERS.get();
        if (buffer == null){
            buffer = ByteBuffer.allocateDirect((int) (Const.K * 17 + 10));
            BUFFERS.set(buffer);
        }
        return buffer;
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

    private long _append(int topic, int queueId, long position, ByteBuffer buffer){
        Queue queue = getQueue(topic, queueId);
        long offset = queue.nextOffset();
        queue.write(position, buffer);
        return offset;
    }

    public long append(int topic, int queueId, ByteBuffer buffer)  {
        Monitor.appendCount ++;
        Monitor.appendSize += buffer.limit();
        if (Monitor.appendCount % 100000 == 0){
            LOGGER.info(Monitor.information());
        }

        Queue queue = getQueue(topic, queueId);
        queue.nextOffset();

        ByteBuffer data = getByteBuffer()
                .put((byte) topic)
                .putShort((short) queueId)
                .putShort((short) buffer.limit())
                .put(buffer);
        data.flip();
        buffer.flip();

        long position = barrier.write(data);
        queue.write(position, buffer);

        barrier.await(5, TimeUnit.SECONDS);
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
