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
            4,                        // 4核分4组刷盘
            100,                // topic总数
            5000,              // queue总数
            (long) (Const.G * 1.90),        // 堆外内存分配
            (long) (Const.G * 3.2),         // 堆内内存分配
            (long) (Const.SECOND * 425)      // killer
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
