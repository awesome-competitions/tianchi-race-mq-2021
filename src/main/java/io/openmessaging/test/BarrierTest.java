package io.openmessaging.test;

import io.openmessaging.impl.Barrier;

import java.util.concurrent.*;

public class BarrierTest {

    public static void main(String[] args) throws InterruptedException, BrokenBarrierException {
        int count = 153 * 10000;
        int batch = 10;

        ThreadPoolExecutor pools = (ThreadPoolExecutor) Executors.newFixedThreadPool(batch);
        CyclicBarrier barrier = new CyclicBarrier(batch);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i ++){
            for (int j = 0; j < batch - 1; j ++){
                pools.execute(()->{
                    try {
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                });
            }
            barrier.await();
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
