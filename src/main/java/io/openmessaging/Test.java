package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Map;

public class Test {

    public static void main(String[] args) {
        MessageQueue mq = new DefaultMessageQueueImpl();
        mq.append("test", 1, ByteBuffer.wrap("hello pmem".getBytes()));
        Map<Integer, ByteBuffer> data = mq.getRange("test", 1, 0, 2);
        System.out.println(data);
    }

}
