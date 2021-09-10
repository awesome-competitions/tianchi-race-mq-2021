package io.openmessaging;

import io.openmessaging.impl.MessageQueueImpl;

import java.nio.ByteBuffer;
import java.util.Map;

public class DefaultMessageQueueImpl extends MessageQueue{

    private MessageQueue queue = new MessageQueueImpl();

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        return queue.append(topic, queueId, data);
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return queue.getRange(topic, queueId, offset, fetchNum);
    }
}
