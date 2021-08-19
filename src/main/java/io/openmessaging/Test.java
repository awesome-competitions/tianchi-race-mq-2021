package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

public class Test {

    private final static int BATCH = 100;

    public static void main(String[] args) {
        test();
    }

    public static void test(){
        long start = System.currentTimeMillis();
        MessageQueue mq = new SSDMessageQueueImpl();

        String[] inputs = new String[BATCH];
        for (int i = 0; i < BATCH; i ++){
            inputs[i] = randomString((int) (Math.random() * 10000));
        }
        for (int i = 0; i < BATCH; i ++){
            mq.append("test", 1, ByteBuffer.wrap(inputs[i].getBytes()));
        }
        for (int i = 0; i < BATCH; i ++){
            Map<Integer, ByteBuffer> data = mq.getRange("test", 1, i, 1);
            if (!Arrays.equals(data.get(0).array(), inputs[i].getBytes())){
                throw new RuntimeException("Correctness error.");
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("io spend time" + (end - start) + "ms");
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
