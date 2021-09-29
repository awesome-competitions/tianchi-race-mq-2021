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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DefaultMessageQueueImpl extends MessageQueue{

//    private final MessageQueue queue = new Mq(new Config(
//            "/essd/",
//            "/pmem/nico",
//            Const.G * 59,
//            Const.G * 54,
//            40,
//            Const.MINUTE * 10 + Const.SECOND * 2
//    ));
    private final MessageQueue queue = null;

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMessageQueueImpl.class);

    public DefaultMessageQueueImpl() throws FileNotFoundException {
        try {
            test();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
//        RandomAccessFile randomAccessFile = new RandomAccessFile("/essd/aof.log", "rw");
//        FileChannel channel = randomAccessFile.getChannel();
//
//        int batch = (int) (Const.K * 6.2);
//        int count = (int) (Const.G * 10 / Const.K / 512);
//
//        ByteBuffer buffer = ByteBuffer.allocate(batch);
//        for (int i = 0; i < buffer.capacity(); i ++){
//            buffer.put((byte) 1);
//        }
//
//        long start = System.currentTimeMillis();
//        for (int i = 0; i < count; i ++){
//            for (int j = 0; j < 80; j ++){
//                buffer.flip();
//                channel.write(buffer);
//            }
//            channel.force(false);
//        }
//        long end = System.currentTimeMillis();
//        LOGGER.info("time {}", end - start);
//        throw new RuntimeException("ex");

        String path = "/pmem/nico";
        long heapSize = Const.G * 59;
        Heap heap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, heapSize);

        long start = System.currentTimeMillis();
//        for (int i = 0; i < 10; i ++){
//            testHeapAllocateAndRW(i, heap);
//        }

        CountDownLatch cdl = new CountDownLatch(40);
        for (int i = 0; i < 40; i ++){
            final int id = i;
            new Thread(()->{
                testHeapAllocate(id, heap);
                cdl.countDown();
            }).start();
        }
        long end = System.currentTimeMillis();
        try {
            cdl.await(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("all spend " + (end - start));
        throw new RuntimeException("ex");
    }
    long heapSize = Const.G * 5;
    void testHeapAllocateAndRW(int id, Heap heap){
        long start = System.currentTimeMillis();
        AnyMemoryBlock block = heap.allocateCompactMemoryBlock(heapSize);
        long end = System.currentTimeMillis();
        System.out.println(id + " allocate " + (end - start));

        start = System.currentTimeMillis();
        for (long i = 0; i < heapSize; i ++){
            block.setByte(i, (byte) 1);
        }
        end = System.currentTimeMillis();
        System.out.println(id + " write " + (end - start));

        start = System.currentTimeMillis();
        for (long i = 0; i < heapSize; i ++){
            block.getByte(i);
        }
        end = System.currentTimeMillis();
        System.out.println(id + " read " + (end - start));
    }

    void testHeapAllocate(int id, Heap heap){
        long start = System.currentTimeMillis();
        heap.allocateMemoryBlock((long) (Const.G * 1.25));
        long end = System.currentTimeMillis();
        System.out.println(id + " allocate " + (end - start));
    }
}
