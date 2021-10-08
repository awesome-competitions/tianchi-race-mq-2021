package io.openmessaging.mq;

import io.openmessaging.MessageQueue;
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

    private final Map<Integer, Queue> queues;

    private final FileWrapper aof;

    private Cache cache;

    private final Loader loader;

    private static final Logger LOGGER = LoggerFactory.getLogger(Mq.class);

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

    void initPools() throws FileNotFoundException {
        Barrier barrier = new Barrier(20, new FileWrapper(new RandomAccessFile(config.getDataDir() + "aof1", "rw")));
        for (int j = 0; j < 20; j ++){
            POOLS.add(barrier);
        }
        barrier = new Barrier(20, new FileWrapper(new RandomAccessFile(config.getDataDir() + "aof2", "rw")));
        for (int j = 0; j < 20; j ++){
            POOLS.add(barrier);
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
        return queues.computeIfAbsent(topic * 10000 + queueId, k -> new Queue(aof, cache));
    }

    public Config getConfig() {
        return config;
    }

    public long append(String topic, int queueId, ByteBuffer buffer)  {
        return append(getTopicId(topic), queueId, buffer);
    }

    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return getRange(getTopicId(topic), queueId, offset, fetchNum);
    }

    private void _append(int topic, int queueId, long position, ByteBuffer buffer){
        Queue queue = getQueue(topic, queueId);
        queue.write(position, buffer);
    }

    public long append(int topic, int queueId, ByteBuffer buffer)  {
//        Monitor.appendCount ++;
        Monitor.appendSize += buffer.limit();
//        if (Monitor.appendCount % 100000 == 0){
//            LOGGER.info(Monitor.information());
//        }

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

    public int getTopicId(String topic){
        switch (topic){
            case "topic1":return 1;
            case "topic2":return 2;
            case "topic3":return 3;
            case "topic4":return 4;
            case "topic5":return 5;
            case "topic6":return 6;
            case "topic7":return 7;
            case "topic8":return 8;
            case "topic9":return 9;
            case "topic10":return 10;
            case "topic11":return 11;
            case "topic12":return 12;
            case "topic13":return 13;
            case "topic14":return 14;
            case "topic15":return 15;
            case "topic16":return 16;
            case "topic17":return 17;
            case "topic18":return 18;
            case "topic19":return 19;
            case "topic20":return 20;
            case "topic21":return 21;
            case "topic22":return 22;
            case "topic23":return 23;
            case "topic24":return 24;
            case "topic25":return 25;
            case "topic26":return 26;
            case "topic27":return 27;
            case "topic28":return 28;
            case "topic29":return 29;
            case "topic30":return 30;
            case "topic31":return 31;
            case "topic32":return 32;
            case "topic33":return 33;
            case "topic34":return 34;
            case "topic35":return 35;
            case "topic36":return 36;
            case "topic37":return 37;
            case "topic38":return 38;
            case "topic39":return 39;
            case "topic40":return 40;
            case "topic41":return 41;
            case "topic42":return 42;
            case "topic43":return 43;
            case "topic44":return 44;
            case "topic45":return 45;
            case "topic46":return 46;
            case "topic47":return 47;
            case "topic48":return 48;
            case "topic49":return 49;
            case "topic50":return 50;
            case "topic51":return 51;
            case "topic52":return 52;
            case "topic53":return 53;
            case "topic54":return 54;
            case "topic55":return 55;
            case "topic56":return 56;
            case "topic57":return 57;
            case "topic58":return 58;
            case "topic59":return 59;
            case "topic60":return 60;
            case "topic61":return 61;
            case "topic62":return 62;
            case "topic63":return 63;
            case "topic64":return 64;
            case "topic65":return 65;
            case "topic66":return 66;
            case "topic67":return 67;
            case "topic68":return 68;
            case "topic69":return 69;
            case "topic70":return 70;
            case "topic71":return 71;
            case "topic72":return 72;
            case "topic73":return 73;
            case "topic74":return 74;
            case "topic75":return 75;
            case "topic76":return 76;
            case "topic77":return 77;
            case "topic78":return 78;
            case "topic79":return 79;
            case "topic80":return 80;
            case "topic81":return 81;
            case "topic82":return 82;
            case "topic83":return 83;
            case "topic84":return 84;
            case "topic85":return 85;
            case "topic86":return 86;
            case "topic87":return 87;
            case "topic88":return 88;
            case "topic89":return 89;
            case "topic90":return 90;
            case "topic91":return 91;
            case "topic92":return 92;
            case "topic93":return 93;
            case "topic94":return 94;
            case "topic95":return 95;
            case "topic96":return 96;
            case "topic97":return 97;
            case "topic98":return 98;
            case "topic99":return 99;
            case "topic100":return 100;
            case "topic101":return 101;
            case "topic102":return 102;
            default: return 103;
        }
    }

}
