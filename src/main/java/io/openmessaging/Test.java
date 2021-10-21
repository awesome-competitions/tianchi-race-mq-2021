package io.openmessaging;

import io.openmessaging.consts.Const;
import io.openmessaging.model.Aof;
import io.openmessaging.model.Config;
import io.openmessaging.model.FileWrapper;
import io.openmessaging.utils.BufferUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Test {

    public static void main(String[] args) throws InterruptedException, IOException {
        FileChannel fileChannel = new RandomAccessFile("abc", "rw").getChannel();
        long start = System.currentTimeMillis();
        long pos = 0;
        long batch = Const.M;
        byte[] bs = new byte[(int) batch];
        for (int i = 0; i < 1024 * 10; i ++){
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, pos, batch);
            mappedByteBuffer.get(bs);
            BufferUtils.clean(mappedByteBuffer);
            pos += batch;
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }


    static void preAllocate(FileChannel channel, long allocateSize) throws IOException {
        if (channel.size() == 0){
            int batch = (int) (Const.M * 4);
            int size = (int) (allocateSize / batch);
            ByteBuffer buffer = ByteBuffer.allocateDirect(batch);
            for (int i = 0; i < batch; i ++){
                buffer.put((byte) 0);
            }
            for (int i = 0; i < size; i ++){
                buffer.flip();
                channel.write(buffer);
            }
            channel.force(true);
            channel.position(0);
            BufferUtils.clean(buffer);
        }
    }
}
