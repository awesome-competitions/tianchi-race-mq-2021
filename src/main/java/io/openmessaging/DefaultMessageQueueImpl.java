package io.openmessaging;

import com.intel.pmem.llpl.*;
import io.openmessaging.consts.Const;
import io.openmessaging.impl.MessageQueueImpl;
import io.openmessaging.model.FileWrapper;
import io.openmessaging.mq.Config;
import io.openmessaging.mq.Mq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DefaultMessageQueueImpl extends MessageQueue{

    private static MessageQueue queue;

    static {
        try {
            queue = new Mq(new Config(
                    "/essd/",
                    "/pmem/nico",
                    (long) (Const.G * 62),
                    40,
                    10,
                    Const.SECOND * 450
            ));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
//    private final MessageQueue queue = null;

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMessageQueueImpl.class);

    public DefaultMessageQueueImpl() throws IOException {
//        try {
//            test();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
//        throw new RuntimeException("END");
        return queue.append(topic, queueId, data);
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return queue.getRange(topic, queueId, offset, fetchNum);
    }

    public void test() throws IOException {
//        testChan60();
//        testChan();
//        testLlpl();
        throw new RuntimeException("ex");
    }

    void testChan60() throws IOException {
        long start = System.currentTimeMillis();
        RandomAccessFile randomAccessFile = new RandomAccessFile("/pmem/aof.log", "rw");
        FileChannel channel = randomAccessFile.getChannel();

        int batch = (int) (Const.K * 32);
        int count = (int) (Const.G * 59 / Const.K / 32);

        long size = 0;

        ByteBuffer buffer = ByteBuffer.allocate(batch);
        for (int i = 0; i < buffer.capacity(); i ++){
            buffer.put((byte) 1);
        }

        for (int i = 0; i < count; i ++){
            buffer.flip();
            channel.write(buffer);

            size += buffer.limit();
            if (size > Const.G){
                LOGGER.info("1");
                size = 0;
            }
        }
        long end = System.currentTimeMillis();
        LOGGER.info("chan time {}", end - start);
    }


    void testChan() throws IOException {
        long start = System.currentTimeMillis();
        RandomAccessFile randomAccessFile = new RandomAccessFile("/pmem/aof.log", "rw");
        FileChannel channel = randomAccessFile.getChannel();

        int batch = (int) (Const.K * 32);
        int count = (int) (Const.G * 10 / Const.K / 32);

        ByteBuffer buffer = ByteBuffer.allocate(batch);
        for (int i = 0; i < buffer.capacity(); i ++){
            buffer.put((byte) 1);
        }

        for (int i = 0; i < count; i ++){
            buffer.flip();
            channel.write(buffer);
        }
        long end = System.currentTimeMillis();
        LOGGER.info("chan time {}", end - start);
    }

    void testLlpl(){
        String path = "/pmem/nico";
        long heapSize = Const.G * 20;
        long start = System.currentTimeMillis();
        Heap heap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
        long end = System.currentTimeMillis();
        System.out.println("create heap " + (end - start));

        start = System.currentTimeMillis();
        AnyMemoryBlock block = heap.allocateCompactMemoryBlock(Const.G * 10);
        end = System.currentTimeMillis();
        System.out.println("allocate " + (end - start));

        start = System.currentTimeMillis();
        byte[] bs = new byte[32 * 1024];
        Arrays.fill(bs, (byte) 1);

        for (long i = 0; i < Const.G * 10; i = i + 32 * 1024){
            block.copyFromArray(bs, 0, i, bs.length);
        }
        end = System.currentTimeMillis();
        System.out.println("write " + (end - start));
    }

    void testHeapAllocate(int id){
        String path = "/pmem/nico" + id;
        long start = System.currentTimeMillis();
        long heapSize = (long) (Const.G * 1.25);
        Heap heap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);
        long end = System.currentTimeMillis();
        System.out.println(id + " open heap " + (end - start));

        start = System.currentTimeMillis();
        heap.allocateMemoryBlock((long) (Const.G * 1));
        end = System.currentTimeMillis();
        System.out.println(id + " allocate " + (end - start));
    }

}
