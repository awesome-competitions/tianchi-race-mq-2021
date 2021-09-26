package io.openmessaging.mq;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.MessageQueue;
import io.openmessaging.model.Config;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

public class Mq extends MessageQueue {

    private Heap heap;

    private Config config;

    private Queue[][] queues;

    private Barrier barrier;

    private FileWrapper aof;

    private FileWrapper tpf;

    private List<ByteBuffer> tbs;

    private Map<String, Integer> ids;

    public Mq(Config config) throws FileNotFoundException {
        this.config = config;
        this.queues = new Queue[101][5001];
        this.aof = new FileWrapper(new RandomAccessFile(config.getDataDir() + "aof", "rw"));
        this.tpf = new FileWrapper(new RandomAccessFile(config.getDataDir() + "tpf", "rw"));
        this.tbs = new ArrayList<>();
        this.barrier = new Barrier(config.getMaxCount(), this.aof);
        this.ids = new ConcurrentHashMap<>();
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        Queue queue = parseQueue(topic, queueId);

        return 0;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return null;
    }

    private int parseTopic(String name){
        return ids.computeIfAbsent(name, n -> Integer.parseInt(name.substring(5)));
    }

    public Queue parseQueue(String topic, int queueId){
        int tid = parseTopic(topic);
        Queue queue = queues[tid][queueId];
        if (queue == null){
            queue = new Queue(tid);
            queues[tid][queueId] = queue;
        }
        return queue;
    }
}
