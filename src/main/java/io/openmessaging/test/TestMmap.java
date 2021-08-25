package io.openmessaging.test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class TestMmap {

    public static void main(String[] args) throws IOException {
        RandomAccessFile raf = new RandomAccessFile("D://test//mmap//test.mmap", "rw");
        MappedByteBuffer mappedByteBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 10000000);
        System.out.println(mappedByteBuffer.getShort());
    }
}
