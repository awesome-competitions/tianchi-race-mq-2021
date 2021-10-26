package io.openmessaging;

import io.openmessaging.mq.Const;
import io.openmessaging.mq.Config;
import io.openmessaging.mq.Mq;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class DefaultMessageQueueImpl extends MessageQueue{

    private final MessageQueue queue = new Mq(new Config(
            "/essd/",
            "/pmem/nico",
            (long) (Const.G * 62),
            40,
            10,
            Const.SECOND * 500
    ));

    public DefaultMessageQueueImpl() throws IOException {
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        return queue.append(topic, queueId, data);
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return queue.getRange(topic, queueId, offset, fetchNum);
    }

}
