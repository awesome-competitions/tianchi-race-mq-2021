package io.openmessaging.test;

import io.openmessaging.MessageQueue;
import io.openmessaging.impl.MessageQueueImpl;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class Test {

    private final static int BATCH = 10000 * 100;
    private final static int QUEUE_SIZE = 5;

    public static void main(String[] args) throws InterruptedException {
        MessageQueueImpl mMapMessageQueue = new MessageQueueImpl();
        List<Supplier<?>> suppliers = new ArrayList<>();

        for (int i = 1; i <= QUEUE_SIZE; i ++){
            suppliers.add(test(mMapMessageQueue, "test1", i));
        }
        for (int i = 1; i <= QUEUE_SIZE; i ++){
            suppliers.add(test(mMapMessageQueue, "test2", i));
        }

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
            String[] inputs = new String[BATCH / 100];
            for (int i = 0; i < inputs.length; i ++){
                inputs[i] = randomString((int) (Math.random() * 100));
            }
            long start = System.currentTimeMillis();
            for (int i = 0; i < BATCH; i ++){
                mq.append(topic, queueId, ByteBuffer.wrap(inputs[i%inputs.length].getBytes()));
            }
            for (int i = 0; i < BATCH; i ++){
                Map<Integer, ByteBuffer> data = mq.getRange(topic, queueId, i, 1);
                if (!Arrays.equals(data.get(0).array(), inputs[i%inputs.length].getBytes())){
                    System.out.println("topic " + topic + ", queue " + queueId + " read fail at " + i);
                    break;
                }
            }
            long end = System.currentTimeMillis();
            System.out.println("topic " + topic + ", queue " + queueId + " spend " + (end - start) + "ms");
            return null;
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
