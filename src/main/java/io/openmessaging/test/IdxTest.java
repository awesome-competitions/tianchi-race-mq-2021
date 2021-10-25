package io.openmessaging.test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class IdxTest {

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
       for (int i = 0; i < Integer.MAX_VALUE; i ++){}
       long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
