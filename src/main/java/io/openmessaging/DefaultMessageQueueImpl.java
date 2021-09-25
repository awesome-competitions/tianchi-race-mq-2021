package io.openmessaging;

import com.intel.pmem.llpl.Accessor;
import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.consts.Const;
import io.openmessaging.impl.MessageQueueImpl;
import io.openmessaging.model.Config;
import io.openmessaging.model.FileWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

public class DefaultMessageQueueImpl extends MessageQueue{

//    private final MessageQueue queue = new MessageQueueImpl();
    private final MessageQueue queue = null;

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMessageQueueImpl.class);

    public DefaultMessageQueueImpl(){
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
        RandomAccessFile randomAccessFile = new RandomAccessFile("/essd/aof.log", "rw");
        FileChannel channel = randomAccessFile.getChannel();

        int batch = (int) (Const.K * 511);
        int count = (int) (Const.G * 10 / batch);

        ByteBuffer buffer = ByteBuffer.allocate(batch);
        for (int i = 0; i < buffer.capacity(); i ++){
            buffer.put((byte) 1);
        }
        buffer.flip();
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i ++){
            channel.write(buffer);
            channel.force(false);
            buffer.flip();
        }
        long end = System.currentTimeMillis();
        LOGGER.info("time {}", end - start);
        throw new RuntimeException("ex");

//        int n = 5;
//        long size = n * Const.G;
//        MemoryBlock block = heap.allocateMemoryBlock(size);
//        byte[] bytes = new byte[1024 * 1024 * 16];
//
//        long start = System.currentTimeMillis();
//        for (int i = 0; i < n * 64; i ++){
//            block.copyFromArray(bytes, 0, i * Const.M * 16, bytes.length);
//        }
//        for (int i = 0; i < n * 64; i ++){
//            block.copyToArray(i * Const.M * 16, bytes, 0, bytes.length);
//        }
//        long end = System.currentTimeMillis();
//        System.out.println((end - start));
    }
}
