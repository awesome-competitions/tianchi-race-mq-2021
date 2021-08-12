package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessageQueueImpl extends MessageQueue {

    public MessageQueueImpl() {
        try {
            Storage.init();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data){
        Storage.Queue queue = Storage.getInstance(topic + "-" + queueId);
        return queue.write(data.array());
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        Storage.Queue queue = Storage.getInstance(topic + "-" + queueId);
        Map<Integer, ByteBuffer> results = new LinkedHashMap<>();
        try {
            byte[][] data = queue.read(offset, fetchNum);
            for(int i = 0; i < data.length; i ++){
                results.put((int) (offset + i), ByteBuffer.wrap(data[i]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }
}
