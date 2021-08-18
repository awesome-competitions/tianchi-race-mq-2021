package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Map;

public class SSDMessageQueueImpl extends MessageQueue{

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        return 0;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return null;
    }
}
