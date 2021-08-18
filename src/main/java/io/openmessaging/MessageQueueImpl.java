package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class MessageQueueImpl extends MessageQueue {

    public MessageQueueImpl() {
        try {
            FileChannelStorage.init();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data){
        FileChannelStorage.Queue queue = FileChannelStorage.getInstance(topic + "-" + queueId);
        return queue.write(data);
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        FileChannelStorage.Queue queue = FileChannelStorage.getInstance(topic + "-" + queueId);
        Map<Integer, ByteBuffer> results = new HashMap<>();
        Storage.Queue queue = Storage.getInstance(topic + "-" + queueId);
        Map<Integer, ByteBuffer> results = new LinkedHashMap<>();
        try {
            byte[][] data = queue.read(offset, fetchNum);
            for(int i = 0; i < data.length; i ++){
                results.put(i, ByteBuffer.wrap(data[i]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }
}
