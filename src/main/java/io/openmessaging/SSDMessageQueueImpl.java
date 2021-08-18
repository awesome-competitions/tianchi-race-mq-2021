package io.openmessaging;

import sun.awt.image.ImageWatched;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SSDMessageQueueImpl extends MessageQueue{

    static final Map<String, Topic> topics = new ConcurrentHashMap<>();
    static final int poolsMaxSize = 10;

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        return 0;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return null;
    }

    public static Topic getTopic(String topic) throws FileNotFoundException {
        return topics.getOrDefault(topic, new Topic(topic));
    }

    public static class Topic{
        private String topic;
        private RandomAccessFile data;
        private RandomAccessFile index;
        private FileChannel writer;
        private Map<Long, FileChannel> readers;
        private Map<Integer, Long> offsets;

        public Topic(String topic) throws FileNotFoundException {
            this.topic = topic;
            this.data = new RandomAccessFile(topic + ".db", "rw");
            this.index = new RandomAccessFile(topic + ".idx", "rw");
            this.writer = data.getChannel();
            this.readers = new ConcurrentHashMap<>();
            this.offsets = new ConcurrentHashMap<>();
        }

        public FileChannel getChannel(Long offset){
            FileChannel channel = readers.get(offset);
            if (channel != null) {
                readers.remove(offset);
                return channel;
            }
            return data.getChannel();
        }

        public void write(int queueId, ByteBuffer data) throws IOException {
            ByteBuffer wrapper = ByteBuffer.allocate(2 + data.capacity());
            wrapper.putShort( (short) data.capacity());
            wrapper.put(data);
            // 更新索引文件
            writer.write(wrapper);
        }
    }
}
