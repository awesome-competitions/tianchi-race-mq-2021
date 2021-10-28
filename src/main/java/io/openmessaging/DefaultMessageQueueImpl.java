package io.openmessaging;

import io.openmessaging.impl.Const;
import io.openmessaging.impl.Config;
import io.openmessaging.impl.Mq;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class DefaultMessageQueueImpl extends MessageQueue{

    private final MessageQueue mq = new Mq(new Config(
            "/essd/",
            Const.G * 132,
            "/pmem/",
            Const.G * 62,
            4,
            100,
            2000,
            (long) (Const.G * 1.91),
            (long) (Const.G * 3.3),
            Const.SECOND * 450
    ));

    public DefaultMessageQueueImpl() throws IOException {
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        return mq.append(topic, queueId, data);
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return mq.getRange(topic, queueId, offset, fetchNum);
    }

}
