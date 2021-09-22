package io.openmessaging;

import io.openmessaging.model.Aof;
import io.openmessaging.model.Config;
import io.openmessaging.model.FileWrapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Test {

    public static void main(String[] args) throws InterruptedException, IOException {
        Aof aof = new Aof(new FileWrapper(new RandomAccessFile("D:\\test\\nio\\abc.txt", "rw")), new Config(
                "", "", 1L ,1 ,1L,1 ,3, 3
        ));

        for (int n = 0; n < 3; n ++){
            new Thread(()->{
                for (int i = 0; i < 100; i ++){
                    try {
                        aof.write(ByteBuffer.wrap(new byte[]{1}));
                        System.out.println(i);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }


        Thread.sleep(10000000);
    }

}
