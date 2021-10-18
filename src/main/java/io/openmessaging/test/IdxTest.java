package io.openmessaging.test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class IdxTest {

    public static void main(String[] args) throws IOException {
        RandomAccessFile idx = new RandomAccessFile("D:\\test\\nio\\test1_100.idx", "rw");
        FileChannel channel = idx.getChannel();

        System.out.println(channel.size());
    }
}
