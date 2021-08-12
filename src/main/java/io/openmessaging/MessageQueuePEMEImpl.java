package io.openmessaging;

import com.intel.pmem.llpl.PersistentAccessor;
import com.intel.pmem.llpl.PersistentHeap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

public class MessageQueuePEMEImpl extends MessageQueue {



    @Override
    public long append(String topic, int queueId, ByteBuffer data){
        new PersistentAccessor(PersistentHeap.createHeap(""));

        return 0;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return null;
    }
}
