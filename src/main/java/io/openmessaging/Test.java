package io.openmessaging;

import com.sun.xml.internal.ws.api.message.Message;

import java.nio.ByteBuffer;

public class Test {

    private final static int BATCH = 10000 * 10;

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        MessageQueue mq = new MessageQueueImpl();
        for (int i = 0; i < BATCH; i ++){
            mq.append("test", 1, ByteBuffer.wrap(("abc" + 1).getBytes()));
        }
        for (int i = 0; i < BATCH; i ++){
            System.out.println(mq.getRange("test", 1, i, 1));
        }
        long end = System.currentTimeMillis();
        System.out.println("spend time" + (end - start) + "ms");
    }

//    public static void main(String[] args) {
//        long start = System.currentTimeMillis();
//        MessageQueue mq = new MessageQueueImpl();
//        for (int i = BATCH / 2; i < BATCH; i ++){
//            System.out.println(mq.getRange("test", 1, i, 1));
//        }
//        long end = System.currentTimeMillis();
//        System.out.println("spend time" + (end - start) + "ms");
//    }
}
