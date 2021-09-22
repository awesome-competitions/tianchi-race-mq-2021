package io.openmessaging.test;

import io.openmessaging.MessageQueue;
import io.openmessaging.consts.Const;
import io.openmessaging.impl.MessageQueueImpl;
import io.openmessaging.model.Config;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class TestWrite {

    private final static int BATCH = 100;
    private final static int TOPIC_SIZE = 50;
    private final static int QUEUE_SIZE = 1;

    public static void main(String[] args) throws InterruptedException {
        MessageQueueImpl mMapMessageQueue = new MessageQueueImpl(new Config(
                "D:\\test\\nio\\",
                null, 1, 30, 2 * Const.K, 100,
                35, Const.K * 288));
        mMapMessageQueue.cleanDB();
        List<Supplier<?>> suppliers = new ArrayList<>();

        for (int i = 1; i <= TOPIC_SIZE; i ++){
            suppliers.add(test(mMapMessageQueue, "topic" + i, i));
        }
//        for (int i = 1; i <= QUEUE_SIZE; i ++){
//            suppliers.add(test(mMapMessageQueue, "test2", i));
//        }

        final CountDownLatch cdl = new CountDownLatch(suppliers.size());
        ExecutorService POOLS = Executors.newFixedThreadPool(suppliers.size());
        for (Supplier<?> supplier : suppliers){
            POOLS.execute(()->{try{supplier.get();} finally {cdl.countDown(); }});
        }
        cdl.await();
        POOLS.shutdown();
    }

    public static Supplier<?> test(MessageQueue mq, String topic, Integer queueId){
        return ()->{
            String s = randomString((int) (Math.random() * 20000) + 1);
            byte[] bytes = s.getBytes();
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            for (;;){
                mq.append(topic, queueId, buffer);
                buffer.flip();
            }
        };
    }

    public static String randomString(int length){
        String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random=new Random();
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<length;i++){
            int number=random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }
}
