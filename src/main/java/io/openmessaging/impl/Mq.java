package io.openmessaging.impl;

import io.openmessaging.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.*;

public class Mq extends MessageQueue{

    private final Aep aep;

    private final Buffers buffers;

    private final Config config;

    private final Queue[][] queues;

    private final Aof[] aofs;

    private final LinkedBlockingQueue<Barrier> barriers = new LinkedBlockingQueue<>();

    private volatile boolean initializedBarriers;

    private static final Logger LOGGER = LoggerFactory.getLogger(Mq.class);

    public Mq(Config config) throws IOException {
        this.config = config;
        this.queues = new Queue[config.getMaxTopicSize()][config.getMaxQueueSize()];
        this.buffers = new Buffers(config.getDirectSize(), config.getHeapSize());
        this.aep = createAep();
        startAepPreallocate();
        this.aofs = initAof();
        startKiller();
    }

    void loadAof(Aof aof) throws IOException {
        long position = 0;
        ByteBuffer header = ByteBuffer.allocate(Const.PROTOCOL_HEADER_SIZE);

        while(true){
            aof.read(position, header);
            position += Const.PROTOCOL_HEADER_SIZE;
            header.flip();
            if (header.remaining() < Const.PROTOCOL_HEADER_SIZE){
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
            if (topic < config.getMaxTopicSize()){
                Queue queue = getQueue(topic, queueId);
                queue.nextOffset();
                queue.getRecords().add(new SSD(aof, position - Const.PROTOCOL_HEADER_SIZE, size));
            }
            position += size;
        }
        preAllocate(aof.getChannel(), config.getAofSize() / config.getBatch());
    }

    void preAllocate(FileChannel channel, long allocateSize) throws IOException {
        if (channel.size() == 0){
            int batch = (int) (Const.M * 4);
            int size = (int) (allocateSize / batch);
            ByteBuffer buffer = ByteBuffer.allocateDirect(batch);
            for (int i = 0; i < batch; i ++){
                buffer.put((byte) 0);
            }
            for (int i = 0; i < size; i ++){
                buffer.flip();
                channel.write(buffer);
            }
            channel.force(true);
            channel.position(0);
            Utils.recycleByteBuffer(buffer);
        }
    }

    void startAepPreallocate(){
        new Thread(()->{
            try {
                LOGGER.info("block start preallocate");
                preAllocate(aep.getChannel(), config.getAepSize());
            } catch (IOException e) {
                try {
                    aep.getChannel().position(0);
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
            LOGGER.info("block preallocate complete");
        }).start();
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

    Aof createAof(String name) throws IOException {
        Aof aof = new Aof(new RandomAccessFile(config.getAofDir() + name, "rw"));
        loadAof(aof);
        return aof;
    }

    Aep createAep() throws IOException {
        return new Aep(new RandomAccessFile(config.getAepDir() + "aep1", "rw"), config.getAepSize());
    }

    Aof[] initAof(){
        Aof[] aofs = new Aof[config.getBatch()];
        for (int i = 0; i < config.getBatch(); i ++){
            try {
                aofs[i] = createAof("aof" + i);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return aofs;
    }

    void initBarriers() {
        synchronized (barriers){
            if (! initializedBarriers){
                try {
                    Thread.sleep(Const.SECOND * 20);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (Threads.size() >= config.getBatch()){
                    int count = Threads.size() / config.getBatch();
                    int surplus = Threads.size() % config.getBatch();
                    for (int i = 0; i < config.getBatch(); i ++){
                        if (i == config.getBatch() - 1){
                            count += surplus;
                        }
                        Barrier barrier = new Barrier(count, aofs[i], aep, buffers);
                        for (int j = 0; j < count; j ++){
                            barriers.add(barrier);
                        }
                    }
                }else{
                    Barrier barrier = new Barrier(Threads.size(), aofs[0], aep, buffers);
                    for (int j = 0; j < Threads.size(); j ++){
                        barriers.add(barrier);
                    }
                }
                initializedBarriers = true;
            }
        }
    }

    public Barrier getBarrier(){
        Threads.Context ctx = Threads.get();
        if (! initializedBarriers){
            initBarriers();
        }
        Barrier barrier = ctx.getBarrier();
        if (barrier == null){
            barrier = barriers.poll();
            ctx.setBarrier(barrier);
        }
        return barrier;
    }

    public Queue getQueue(int topic, int queueId){
        Queue queue = queues[topic][queueId];
        if (queue == null){
            queue = new Queue();
            queues[topic][queueId] = queue;
        }
        return queue;
    }

    public long append(String topic, int queueId, ByteBuffer buffer)  {
        return append(getTopicId(topic), queueId, buffer);
    }

    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return getRange(getTopicId(topic), queueId, offset, fetchNum);
    }

    public long append(int topic, int queueId, ByteBuffer buffer)  {
        Queue queue = getQueue(topic, queueId);
        long offset = queue.nextOffset();

        Barrier barrier = getBarrier();
        long aos = barrier.write(topic, queueId, offset, buffer);
        long pos;
        try {
            barrier.await(30, TimeUnit.SECONDS);
            pos = barrier.getSsdPosition() + aos;
        } catch (BrokenBarrierException e) {
            buffer.flip();
            pos = barrier.writeAndFsync(topic, queueId, offset, buffer);
        }
        buffer.flip();

        Data data;
        if (barrier.aepEnable()){
            data = new PMem(barrier.getAep(), barrier.getAepPosition() + aos + Const.PROTOCOL_HEADER_SIZE, buffer.limit());
            queue.getRecords().add(data);
            return offset;
        }

        data = buffers.allocateBuffer(buffer.limit());
        if (data != null){
            data.set(buffer);
            queue.getRecords().add(data);
            return offset;
        }
        queue.write(barrier.getAof(), pos, buffer);
        return offset;
    }


    public Map<Integer, ByteBuffer> getRange(int topic, int queueId, long offset, int fetchNum) {
        Queue queue = getQueue(topic, queueId);
        return queue.read(offset, fetchNum);
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
            default: return 0;
        }
    }

}
