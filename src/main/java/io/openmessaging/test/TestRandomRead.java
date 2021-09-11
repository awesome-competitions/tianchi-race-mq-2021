package io.openmessaging.test;

import io.openmessaging.MessageQueue;
import io.openmessaging.impl.MessageQueueImpl;
import io.openmessaging.model.Config;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class TestRandomRead {

    private final static int BATCH = 10000 * 10;
    private final static int PARALLEL_SIZE = 2;

    public static void main(String[] args) throws InterruptedException {
        MessageQueueImpl mMapMessageQueue = new MessageQueueImpl(new Config("D:\\test\\nio\\", 1024 * 1024 * 16, 10, 1, 1));
        List<Supplier<?>> suppliers = new ArrayList<>();
        Map<Long, Integer> results = new ConcurrentHashMap<>();

        LinkedBlockingQueue<Integer> msgQueue = new LinkedBlockingQueue<>();
        for (int i = 0; i < BATCH; i ++){
            msgQueue.add(i);
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i <= PARALLEL_SIZE; i ++){
            suppliers.add(test(msgQueue, mMapMessageQueue, results,"test3", 1));
        }
        final CountDownLatch cdl = new CountDownLatch(suppliers.size());
        ExecutorService POOLS = Executors.newFixedThreadPool(suppliers.size());
        for (Supplier<?> supplier : suppliers){
            POOLS.execute(()->{try{supplier.get();} finally {cdl.countDown(); }});
        }
        cdl.await();

        for (int i = 0; i < BATCH; i ++){
            long offset = (long) (Math.random() * BATCH);
            Map<Integer, ByteBuffer> data = mMapMessageQueue.getRange("test3", 1, offset, 1);
            if (! Objects.equals(new String(data.get(0).array()), String.valueOf(results.get((long) offset)))){
                throw new RuntimeException("err at " + offset);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("spend time " + (end - start) + "ms");
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
