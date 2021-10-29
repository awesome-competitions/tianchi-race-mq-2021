package io.openmessaging.test;

import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPoolTest {

    public static void main(String[] args) throws InterruptedException {
        int count = 120 * 10000;
        int batch = 10;

        ThreadPoolExecutor pools = (ThreadPoolExecutor) Executors.newFixedThreadPool(batch);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i ++){
            for (int j = 0; j < batch; j ++){
                pools.execute(()->{});
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
