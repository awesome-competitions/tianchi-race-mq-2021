package io.openmessaging.test;

import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class LinkedBlockQueueTest {

    public static void main(String[] args) throws InterruptedException {
        int count = 100 * 10000;

        LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue();
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i ++){
            queue.add(1);
            queue.poll();
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);


    }
}
