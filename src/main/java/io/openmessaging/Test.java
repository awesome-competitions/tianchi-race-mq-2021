package io.openmessaging;

import io.openmessaging.model.Aof;
import io.openmessaging.model.Config;
import io.openmessaging.model.FileWrapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Test {

    public static void main(String[] args) throws InterruptedException, IOException {
        HashMap<Integer, Object> hashMap = new HashMap<>(100);
        for (int i = 0; i < 100; i ++){
            hashMap.put(i, i);
        }
        System.out.println(hashMap.put(1,1));
        System.out.println(hashMap);
    }

}
