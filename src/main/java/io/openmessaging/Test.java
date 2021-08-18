package io.openmessaging;

import java.nio.ByteBuffer;

public class Test {

    private final static int BATCH = 10000 * 100;

    public static void main(String[] args) {
        testFC();
    }

    public static void testFC(){
        long start = System.currentTimeMillis();
        MessageQueue mq = new MessageQueueImpl();
        for (int i = 0; i < BATCH; i ++){
            mq.append("test", 1, ByteBuffer.wrap(("abc" + 1).getBytes()));
        }
        for (int i = 0; i < BATCH; i ++){
            mq.getRange("test", 1, i, 1);
        }
        long end = System.currentTimeMillis();
        System.out.println("io spend time" + (end - start) + "ms");
    }
}
