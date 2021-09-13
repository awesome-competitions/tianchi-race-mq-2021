package io.openmessaging;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.consts.Const;
import io.openmessaging.impl.MessageQueueImpl;
import io.openmessaging.model.Config;

import java.nio.ByteBuffer;
import java.util.Map;

public class DefaultMessageQueueImpl extends MessageQueue{

    private final MessageQueue queue = new MessageQueueImpl(new Config("", 1, 1, 1, 1));

    public DefaultMessageQueueImpl(){
        test();
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        throw new RuntimeException("test");
//        return queue.append(topic, queueId, data);
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return queue.getRange(topic, queueId, offset, fetchNum);
    }

    public void test(){
        Heap heap = Heap.createHeap("/pmem/nico1");
        long size = 16 * Const.G;
        MemoryBlock block = heap.allocateMemoryBlock(size);
        byte[] bytes = new byte[1024 * 1024 * 16];

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1024; i ++){
            block.copyFromArray(bytes, 0, i * Const.M * 16, bytes.length);
        }
        for (int i = 0; i < 1024; i ++){
            block.copyToArray(i * Const.M * 16, bytes, 0, bytes.length);
        }
        long end = System.currentTimeMillis();
        System.out.println((end - start));
    }
}
