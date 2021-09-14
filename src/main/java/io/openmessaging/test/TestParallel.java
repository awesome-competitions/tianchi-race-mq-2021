package io.openmessaging.test;

import io.openmessaging.MessageQueue;
import io.openmessaging.consts.Const;
import io.openmessaging.impl.MessageQueueImpl;
import io.openmessaging.model.Config;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class TestParallel {

    private final static int BATCH = 10000 * 10;
    private final static int PARALLEL_SIZE = 10;

    public static void main(String[] args) throws InterruptedException {
        MessageQueueImpl mMapMessageQueue = new MessageQueueImpl(new Config("D:\\test\\nio\\", 64 * Const.K, 1));
        mMapMessageQueue.cleanDB();
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
            Map<Integer, ByteBuffer> data = mMapMessageQueue.getRange("test3", 1, i, 1);
            if (! Objects.equals(new String(data.get(0).array()), String.valueOf(results.get((long) i)))){
                throw new RuntimeException("err at " + i);
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
