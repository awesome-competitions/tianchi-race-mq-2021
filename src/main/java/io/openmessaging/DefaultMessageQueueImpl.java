package io.openmessaging;

import com.intel.pmem.llpl.Accessor;
import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.consts.Const;
import io.openmessaging.impl.MessageQueueImpl;
import io.openmessaging.model.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;

public class DefaultMessageQueueImpl extends MessageQueue{

//    private final MessageQueue queue = new MessageQueueImpl();
    private final MessageQueue queue = null;

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMessageQueueImpl.class);

    public DefaultMessageQueueImpl(){
        test();
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        throw new RuntimeException("END");
//        return queue.append(topic, queueId, data);
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return queue.getRange(topic, queueId, offset, fetchNum);
    }

    public void test(){
        String path = "/pmem/nico";
        Heap heap = Heap.exists(path) ? Heap.openHeap(path) : Heap.createHeap(path, 59 * Const.G);
        long start = System.currentTimeMillis();
        for (int i = 0; i < 500000; i++){
           heap.allocateMemoryBlock(Const.K * 96);
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
