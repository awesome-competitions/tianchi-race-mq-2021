package io.openmessaging.test;

import io.openmessaging.MessageQueue;
import io.openmessaging.impl.MessageQueueImpl;
import io.openmessaging.model.Config;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class Test {

    private final static int BATCH = 100;
    private final static int QUEUE_SIZE = 10;

    public static void main(String[] args) throws InterruptedException {
        MessageQueueImpl mMapMessageQueue = new MessageQueueImpl(new Config("D:\\test\\nio\\", 1024 * 1024 * 16, 10, QUEUE_SIZE / 10, QUEUE_SIZE));
        mMapMessageQueue.cleanDB();
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
            String[] inputs = new String[BATCH / 1];
            for (int i = 0; i < inputs.length; i ++){
                inputs[i] = randomString((int) (Math.random() * 100) + 1);
//                inputs[i] = randomString(1);
            }
            long start = System.currentTimeMillis();
            for (int i = 0; i < BATCH; i ++){
                mq.append(topic, queueId, ByteBuffer.wrap(inputs[i%inputs.length].getBytes()));
            }
            long end = System.currentTimeMillis();
            System.out.println("【write】 topic " + topic + ", queue " + queueId + " spend " + (end - start) + "ms");

            start = System.currentTimeMillis();
            for (int i = 0; i < BATCH; i ++){
                try{
                    Map<Integer, ByteBuffer> data = mq.getRange(topic, queueId, i, 1);
                    if (!Arrays.equals(data.get(0).array(), inputs[i%inputs.length].getBytes())){
                        System.out.println("topic " + topic + ", queue " + queueId + " read fail at " + i);
                        break;
                    }
                }catch (Exception e){
                    System.out.println(i);
                    throw e;
                }
            }
            end = System.currentTimeMillis();
            System.out.println("【read】 topic " + topic + ", queue " + queueId + " spend " + (end - start) + "ms");

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
