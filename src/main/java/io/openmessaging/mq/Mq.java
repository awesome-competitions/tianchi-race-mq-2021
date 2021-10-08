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

    private Cache cache;

    private final Loader loader;

    private static final Logger LOGGER = LoggerFactory.getLogger(Mq.class);

    private final Map<String, Integer> TID = new ConcurrentHashMap<>();

    private final LinkedBlockingQueue<Barrier> POOLS = new LinkedBlockingQueue<>();

    public Mq(Config config) throws IOException {
        LOGGER.info("Mq init");
        this.config = config;
        this.queues = new ConcurrentHashMap<>();
        this.aof = new FileWrapper(new RandomAccessFile(config.getDataDir() + "aof", "rw"));
//        this.cache = new Cache(config.getHeapDir(), config.getHeapSize());
        this.loader = new Loader(aof, cache, queues);
//        loadAof();
        initPools();
        startKiller();
        LOGGER.info("Mq completed");
    }

    void loadAof() throws IOException {
        long position = 0;
        ByteBuffer header = ByteBuffer.allocate(9);
        while(true){
            aof.read(position, header);
            position += 9;
            header.flip();
            if (header.remaining() < 9){
                break;
            }
            int topic = header.get();
            int queueId = header.getShort();
            int offset = header.getInt();
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
                    LOGGER.info("killed: " + Monitor.information());
                    System.exit(-1);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    void initPools(){
        for (int i = 0; i < 10; i ++){
            Barrier barrier = new Barrier(config.getMaxCount(), aof);
            for (int j = 0; j < config.getMaxCount(); j ++){
                POOLS.add(barrier);
            }
        }
    }

    public Barrier getBarrier(){
        Threads.Context ctx = Threads.get();
        Barrier barrier = ctx.getBarrier();
        if (barrier == null){
            barrier = POOLS.poll();
            ctx.setBarrier(barrier);
            if (barrier != null){
                barrier.register(ctx);
            }
        }
        return barrier;
    }

    public Queue getQueue(int topic, int queueId){
        return queues.computeIfAbsent(topic, k -> new HashMap<>()).computeIfAbsent(queueId, k -> new Queue(aof, cache));
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

    private void _append(int topic, int queueId, long position, ByteBuffer buffer){
        Queue queue = getQueue(topic, queueId);
        long offset = queue.nextOffset();
        queue.write(position, buffer);
    }

    public long append(int topic, int queueId, ByteBuffer buffer)  {
        Monitor.appendCount ++;
        Monitor.appendSize += buffer.limit();
        if (Monitor.appendCount % 100000 == 0){
            LOGGER.info(Monitor.information());
        }

        Queue queue = getQueue(topic, queueId);
        long offset = queue.nextOffset();

        Threads.Context ctx = Threads.get();

        ByteBuffer data = ctx.getBuffer()
                .put((byte) topic)
                .putShort((short) queueId)
                .putInt((int) offset)
                .putShort((short) buffer.limit())
                .put(buffer);
        data.flip();
        ctx.setReadyWrite(true);
        buffer.flip();

        Barrier barrier = getBarrier();
        long position = barrier.await(20, TimeUnit.SECONDS);
        if (position == -1){
            position = barrier.getPosition() + ctx.getSsdPos();
        }
//
//        if(! queue.write(position, buffer)){
//            loader.setPosition(position);
//        }
        return queue.getOffset();
    }

    public Map<Integer, ByteBuffer> getRange(int topic, int queueId, long offset, int fetchNum) {
        loader.start();

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
