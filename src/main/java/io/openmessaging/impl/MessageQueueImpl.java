package io.openmessaging.impl;

import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import io.openmessaging.MessageQueue;
import io.openmessaging.cache.Cache;
import io.openmessaging.consts.Const;
import io.openmessaging.model.*;
import io.openmessaging.model.Queue;
import io.openmessaging.utils.ArrayUtils;
import io.openmessaging.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MessageQueueImpl extends MessageQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageQueueImpl.class);

    private final Config config;
    private final Cache cache;
    private final Map<String, Topic> topics;

    public MessageQueueImpl() {
        this(new Config(
                "/essd/",
                "/pmem/nico",
                Const.G * 59,
                (int) (Const.M * 64),
                (int) (Const.G * 55 / (Const.M * 64)),
                10,
                10000)
        );
    }

    public MessageQueueImpl(Config config) {
        LOGGER.info("start");
        lsPmem();
        this.config = config;
        this.topics = new ConcurrentHashMap<>();
        this.cache = new Cache(config.getHeapDir(), config.getHeapSize(), config.getCacheSize(), config.getPageSize());
    }

    public void cleanDB(){
        File root = new File(config.getDataDir());
        if (root.exists() && root.isDirectory()){
            if (ArrayUtils.isEmpty(root.listFiles())) return;
            for (File file: Objects.requireNonNull(root.listFiles())){
                if (file.exists() && ! file.isDirectory() && file.delete()){ }
            }
            Map<String, Map<Integer, File>> dbs = new HashMap<>();
            Map<String, Map<Integer, File>> ids = new HashMap<>();
            Map<String, Map<Integer, File>> cur;
            if (ArrayUtils.isNotEmpty(root.listFiles())){
                for (File file: Objects.requireNonNull(root.listFiles())){
                    if (! file.isDirectory()) {
                        String[] infos = file.getName().substring(0, file.getName().lastIndexOf(".")).split("_");
                        cur = file.getName().endsWith(".db") ? dbs : ids;
                        cur.computeIfAbsent(infos[0], k -> new HashMap<>()).put(Integer.parseInt(infos[1]), file);
                    }
                }
            }
        }
    }

    public void loadDB(){
        File root = new File(config.getDataDir());
        if (! root.exists() && ! root.mkdirs() && ! root.isDirectory()){
            throw new RuntimeException("load db error");
        }
    }

    public void lsPmem(){
        File root = new File("/pmem/");
        if (root.exists() && root.isDirectory()){
            if (ArrayUtils.isEmpty(root.listFiles())) return;
            for (File file: Objects.requireNonNull(root.listFiles())){
                LOGGER.info("file {}", file.getPath());
            }
        }
    }

    long size = 0;
    int count = 0;
    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        try {
            ++count;
            size += data.capacity();
            if (count % 100000 == 0){
                LOGGER.info("write count {}, size {}", count, size);
            }
            if (count > 500){
                LOGGER.info("end");
                LOGGER.info("write count {}, size {}", count, size);
                throw new RuntimeException("stop");
            }
            LOGGER.info("write topic {}, queueId {}", topic, queueId);
            return getTopic(topic).write(queueId, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    int readCount = 0;
    int times = 0;
    @Override
    public Map<Integer, ByteBuffer> getRange(String name, int queueId, long offset, int fetchNum) {
        try {
            readCount += fetchNum;
            if (readCount > 20000){
                int time = ++times;
                LOGGER.info("read count {}, times {}", readCount, time);
                readCount = 0;
            }
            Topic topic = getTopic(name);
            List<ByteBuffer> results = topic.read(queueId, offset, fetchNum);
            if (CollectionUtils.isEmpty(results)){
                return null;
            }
            Map<Integer, ByteBuffer> byteBuffers = new HashMap<>();
            for(int i = 0; i < fetchNum; i ++){
                byteBuffers.put(i, i < results.size() ? results.get(i): null);
            }
            return byteBuffers;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public synchronized Topic getTopic(String name) throws FileNotFoundException {
        Topic topic = topics.get(name);
        if (topic == null){
            topic = new Topic(name, config, cache);
            topics.put(name, topic);
        }
        return topic;
    }

}
