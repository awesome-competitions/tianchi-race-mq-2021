package io.openmessaging.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

public class Test {

    public static void main(String[] args) throws InterruptedException {
        int count = 120 * 10000;
        int batch = 10;

        ThreadPoolExecutor pools = (ThreadPoolExecutor) Executors.newFixedThreadPool(batch);
        Semaphore semaphore = new Semaphore(0);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i ++){
            for (int j = 0; j < batch; j ++){
                pools.execute(semaphore::release);
            }
            semaphore.acquire(batch);
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
