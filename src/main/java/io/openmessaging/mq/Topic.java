package io.openmessaging.mq;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Topic {

    private final Map<Integer, Queue> queues;

    private final FileWrapper tpl;

    private final AtomicLong page;

    private final int pageSize;

    public Topic(FileWrapper tpl, int pageSize) {
        this.tpl = tpl;
        this.queues = new HashMap<>();
        this.page = new AtomicLong();
        this.pageSize = pageSize;
    }

    public Queue getQueue(int queueId, Cache cache, FileWrapper aof){
        return queues.computeIfAbsent(queueId, k -> {
            return new Queue(this, cache, aof);
        });
    }

    public SSDBlock nextSSDBlock(){
        return new SSDBlock(tpl, page.getAndIncrement() * pageSize, pageSize);
    }
}
