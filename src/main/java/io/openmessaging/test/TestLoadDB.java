package io.openmessaging.test;

import io.openmessaging.MessageQueue;
import io.openmessaging.consts.Const;
import io.openmessaging.impl.MessageQueueImpl;
import io.openmessaging.model.Config;
import io.openmessaging.mq.Mq;
import io.openmessaging.utils.ArrayUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class TestLoadDB {



    private final static int BATCH = 1000;
    private final static int PARALLEL_SIZE = 1;
    private final static String DIR = "D://test//nio//";
    private final static String HEAP_DIR = null;

    public static Mq getMq(int count) throws IOException {
        return new Mq(new io.openmessaging.mq.Config(DIR, null, 0, count, (int) (Const.K * 512), 0));
    }

    public static void cleanDB(){
        File root = new File(DIR);
        if (root.exists() && root.isDirectory()){
            if (ArrayUtils.isEmpty(root.listFiles())) return;
            for (File file: Objects.requireNonNull(root.listFiles())){
                if (file.exists() && ! file.isDirectory() && file.delete()){ }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        cleanDB();
        MessageQueue mMapMessageQueue = getMq(1);
        List<Supplier<?>> suppliers = new ArrayList<>();
        Map<Long, Integer> results = new ConcurrentHashMap<>();

        LinkedBlockingQueue<Integer> msgQueue = new LinkedBlockingQueue<>();
        for (int i = 0; i < BATCH; i ++){
            msgQueue.add(i);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < PARALLEL_SIZE; i ++){
            suppliers.add(test(msgQueue, mMapMessageQueue, results,"topic3", 1));
        }
        final CountDownLatch cdl = new CountDownLatch(suppliers.size());
        ExecutorService POOLS = Executors.newFixedThreadPool(suppliers.size());
        for (Supplier<?> supplier : suppliers){
            POOLS.execute(()->{try{supplier.get();} finally {cdl.countDown(); }});
        }
        cdl.await();

        for (int i = 0; i < BATCH; i ++){
            Map<Integer, ByteBuffer> data = mMapMessageQueue.getRange("topic3", 1, i, 1);
            if (! Objects.equals(new String(data.get(0).array()), String.valueOf(results.get((long) i)))){
                throw new RuntimeException("err at " + i);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("p1 spend time " + (end - start) + "ms");


        MessageQueue mMapMessageQueueNew = getMq(2);
        for (int i = 0; i < BATCH; i ++){
            Map<Integer, ByteBuffer> data = mMapMessageQueueNew.getRange("topic3", 1, i, 1);
            if (data.isEmpty()){
                throw new RuntimeException("err at " + i);
            }
            if (! Objects.equals(new String(data.get(0).array()), String.valueOf(results.get((long) i)))){
                throw new RuntimeException("err at " + i);
            }
        }


        long offset = mMapMessageQueueNew.append("topic3", 1, ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));
        Map<Integer, ByteBuffer> values = mMapMessageQueueNew.getRange("topic3", 1, offset, 1);
        System.out.println(offset + ":" + new String(values.get(0).array()));

        POOLS.shutdown();
    }

    public static Supplier<?> test(LinkedBlockingQueue<Integer> msgQueue, MessageQueue mq, Map<Long, Integer> results, String topic, Integer queueId){
        return ()->{
            Integer msg = null;
            while((msg = msgQueue.poll()) != null){
                long offset = mq.append(topic, queueId, ByteBuffer.wrap(String.valueOf(msg).getBytes()));
                results.put(offset, msg);
            }
            return null;
        };
    }
}
