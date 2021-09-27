package io.openmessaging.mq;

import java.util.Objects;

public class Key {

    private String topic;

    private int queueId;

    private long offset;

    public Key(String topic, int queueId, long offset) {
        this.topic = topic;
        this.queueId = queueId;
        this.offset = offset;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Key key = (Key) o;
        return queueId == key.queueId && offset == key.offset && Objects.equals(topic, key.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, queueId, offset);
    }
}
