package io.openmessaging.mq;

import io.openmessaging.consts.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class Loader {

    private final FileWrapper aof;

    private final Cache cache;

    private volatile boolean start;

    private final Map<Integer, Map<Integer, Queue>> queues;

    private final ReentrantLock lock = new ReentrantLock();

    private final Logger LOGGER = LoggerFactory.getLogger(Loader.class);

    public Loader(FileWrapper aof, Cache cache, Map<Integer, Map<Integer, Queue>> queues) {
        this.aof = aof;
        this.cache = cache;
        this.queues = queues;
    }

    public void start(long position){
        if (!start){
            lock.lock();
            if (!start){
                start = true;
                new Thread(() -> {
                    startLoad(position);
                }).start();
                start = true;
            }
            lock.unlock();
        }
    }

    // 55 - 75 = 20
    public void startLoad(long position){
        LOGGER.info("start loader");

        int batch = (int) (Const.M * 4);
        ByteBuffer buffer = ByteBuffer.allocateDirect(batch);
        long endPos = position + Const.G * 20;
        long startPos = position;
        while (startPos < endPos){
            try {
                aof.read(position, buffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            buffer.flip();

            while (buffer.remaining() > 9){
                int topic = buffer.get();
                int queueId = buffer.getShort();
                long offset = buffer.getInt();
                int size = buffer.getShort();
                if (buffer.remaining() < size){
                    buffer.position(buffer.position() - 9);
                    break;
                }
                Queue queue = queues.get(topic).get(queueId);
                if (queue.getRecords().get(offset) instanceof PMem || queue.getNextReadOffset() > offset){
                    buffer.position(buffer.position() + size);
                    continue;
                }

                byte[] bytes = new byte[size];
                buffer.get(bytes);
                Data data = cache.take();
                data.set(ByteBuffer.wrap(bytes));
                queue.getRecords().put(offset, data);
            }

            startPos += buffer.position();
            buffer.clear();
        }




    }
}
