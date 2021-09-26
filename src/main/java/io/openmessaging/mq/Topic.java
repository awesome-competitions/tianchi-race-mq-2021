package io.openmessaging.mq;

import java.util.HashMap;
import java.util.Map;

public class Topic {

    private final Lru<Integer, Queue> queues;

    private long size;

    public Topic() {
        this.queues = new Lru<>(5001);
    }

    public Lru<Integer, Queue> getQueues() {
        return queues;
    }

    public Queue getQueue(int queueId){
        return queues.computeIfAbsent(queueId, Queue::new);
    }

    public void refresh(Queue queue){
        queues.put(queue.getId(), queue);
    }

    public void increment(long capacity){
        this.size += capacity;
    }

    public void decrement(long capacity){
        this.size -= capacity;
    }

    public long getSize() {
        return size;
    }
}
