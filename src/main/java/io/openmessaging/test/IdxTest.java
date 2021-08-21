package io.openmessaging.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;

public class IdxTest {

    public static void main(String[] args) throws IOException {
        RandomAccessFile idx = new RandomAccessFile("D:\\test\\mmap\\test.idx", "r");
        FileChannel channel = idx.getChannel();

        ByteBuffer index = ByteBuffer.allocate(26);
        while (channel.read(index) > 0){
            index.flip();
            System.out.println(index.getShort() + " " + index.getLong() + " " + index.getLong() + " " + index.getLong());
            index.clear();
        }
    }
}
