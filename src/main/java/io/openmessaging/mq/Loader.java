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

    private long position = -1;

    private final Map<Integer, Map<Integer, Queue>> queues;

    private final ReentrantLock lock = new ReentrantLock();

    private final Logger LOGGER = LoggerFactory.getLogger(Loader.class);

    public Loader(FileWrapper aof, Cache cache, Map<Integer, Map<Integer, Queue>> queues) {
        this.aof = aof;
        this.cache = cache;
        this.queues = queues;
    }

    public void start(){
        if (!start){
            lock.lock();
            if (!start){
                start = true;
                new Thread(this::startLoad).start();
            }
            lock.unlock();
        }
    }

    public void setPosition(long pos){
        if (position == -1){
            lock.lock();
            if (position == -1){
                this.position = pos;
            }
            lock.unlock();
        }
    }

    // 55 - 75 = 20
    private void startLoad(){
        LOGGER.info("start loader, position {}", position);

        ByteBuffer tmp = ByteBuffer.allocate((int) (Const.K * 17));

        int batch = (int) (Const.M);
        ByteBuffer buffer = ByteBuffer.allocateDirect(batch);
        long endPos = position + Const.G * 20;
        long startPos = position;
        long loadSize = 0;
        int count = 0;
        while (startPos < endPos){
            if (loadSize > Const.G){
                LOGGER.info("load {} G", ++count);
                loadSize = 0;
            }
            try {
                aof.read(startPos, buffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            buffer.flip();

            while (buffer.remaining() > 9){
                int topic = buffer.get();
                int queueId = buffer.getShort();
                long offset = buffer.getInt();
                int size = buffer.getShort();
                if (buffer.remaining() < size || size == 0){
                    buffer.position(buffer.position() - 9);
                    break;
                }
                Queue queue = queues.get(topic).get(queueId);
                if (queue.getRecords().get(offset) instanceof PMem || queue.getNextReadOffset() > offset){
                    buffer.position(buffer.position() + size);
                    continue;
                }

                buffer.limit(buffer.position() + size);
                tmp.put(buffer);
                buffer.limit(buffer.capacity());
                tmp.flip();
                Data data = cache.take();
                data.set(tmp);
                tmp.clear();

                Monitor.swapSSDToPmemCount ++;
                queue.getRecords().put(offset, data);
            }

            startPos += buffer.position();
            loadSize += buffer.position();
            buffer.clear();
        }

        LOGGER.info("end loader, position {}", startPos);
    }
}
