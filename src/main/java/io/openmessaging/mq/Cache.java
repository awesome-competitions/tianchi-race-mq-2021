package io.openmessaging.mq;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.consts.Const;
import io.openmessaging.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Cache {

    private Mq mq;

    private Heap heap;

    private Block active;

    private AtomicLong aofPos;

    private final List<Block> blocks = new ArrayList<>(10);

    private final LinkedBlockingQueue<Block> idles = new LinkedBlockingQueue<>();

    private final Map<Integer, Block> stables = new ConcurrentHashMap<>();

    private final ThreadLocal<Integer> blockPos = new ThreadLocal<>();

    private static final long ACTIVE_SIZE = Const.G * 3;

    private static final long BLOCK_SIZE = Const.G * 2;

    private static final long LOAD_SIZE = Const.K * 512;

    private static final Logger LOGGER = LoggerFactory.getLogger(Cache.class);

    public Cache(Mq mq){
        this.mq = mq;
        Config config = mq.getConfig();
        if (config.getHeapDir() != null){
            this.heap = Heap.exists(config.getHeapDir()) ? Heap.openHeap(config.getHeapDir()) : Heap.createHeap(config.getHeapDir(), config.getHeapSize());
            this.active = applyBlock(0, ACTIVE_SIZE);
            this.blocks.add(applyBlock(1, BLOCK_SIZE));
            this.aofPos = new AtomicLong();
            startProducer();
            startLoader();
            startBlockMonitor();
        }
    }

    private void startProducer(){
        Thread producer = new Thread(() -> {
            for (int i = 0; i < 24; i ++){
                this.blocks.add(applyBlock(2 + i, BLOCK_SIZE));
            }
        });
        producer.setDaemon(true);
        producer.start();
    }

    private void startBlockMonitor(){
        Thread monitor = new Thread(() -> {
            while (true){
                try {
                    Thread.sleep(60 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Iterator<Map.Entry<Integer, Block>> iterator = stables.entrySet().iterator();
                List<Integer> removed = null;
                while (iterator.hasNext()){
                    Map.Entry<Integer, Block> entry = iterator.next();
                    Block block = entry.getValue();
                    LOGGER.info("block {}, offsets size {}", block.getId(), block.getOffsets().size());
                }
            }
        });
        monitor.setDaemon(true);
        monitor.start();
    }

    private void startLoader(){
        Thread loader = new Thread(()->{
            while (true){
                try {
                    Block block = idles.take();
                    block.reset();

                    Monitor.loadTimes ++;
                    LOGGER.info("start load, start pos {}, block {}", aofPos, block.getId());

                    long start = System.currentTimeMillis();
                    boolean finished = false;
                    while(! finished && aofPos.longValue() + LOAD_SIZE < mq.getAof().position()){
                        ByteBuffer buffer = ByteBuffer.allocate((int) LOAD_SIZE);
                        mq.getAof().read(aofPos.longValue(), buffer);
                        buffer.flip();

                        while(buffer.remaining() > 5){
                            int tid = buffer.get();
                            int qid = buffer.getShort();
                            long offset = buffer.getInt();
                            int size = buffer.getShort();
                            if (buffer.remaining() < size){
                                break;
                            }
                            byte[] bytes = new byte[size];
                            buffer.get(bytes);

                            long memPos = block.allocate(size);
                            if (memPos == -1){
                                finished = true;
                                break;
                            }

                            Queue queue = this.mq.getQueues().get(tid).get(qid);
                            Data data = new PMem(block, memPos, size);
                            data.set(ByteBuffer.wrap(bytes));
                            queue.getRecords().put(offset, new PMem(block, memPos, size));
                            block.register(tid, qid, offset);
                            this.aofPos.addAndGet(9 + size);
                        }
                    }
                    long end = System.currentTimeMillis();
                    Monitor.loadSpend += (end - start);
                    if (! block.isFree()){
                        stables.put(block.getId(), block);
                    }
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                }
            }
        });
        loader.setDaemon(true);
        loader.start();
    }

    public Block applyBlock(int id, long size){
        return new Block(id, heap.allocateCompactMemoryBlock(size), size);
    }

    public Block localBlock(){
        if (blockPos.get() < blocks.size()){
            return blocks.get(blockPos.get());
        }
        return null;
    }

    public void unregister(int tid, int qid, long offset){
        Iterator<Map.Entry<Integer, Block>> iterator = stables.entrySet().iterator();
        List<Integer> removed = null;
        while (iterator.hasNext()){
            Map.Entry<Integer, Block> entry = iterator.next();
            Block block = entry.getValue();
            block.unregister(tid, qid, offset);
            if (block.isFree()){
                if (removed == null){
                    removed = new ArrayList<>();
                }
                removed.add(entry.getKey());
            }
        }
        if (CollectionUtils.isNotEmpty(removed)){
            for (Integer key: removed){
                Block block = stables.remove(key);
                if (block != null){
                    idles.add(block);
                }
            }
        }
    }

    public Data allocate(long aofPosition, int cap){
        if (heap == null){
            return new Dram(cap);
        }
        if (blockPos.get() == null){
            blockPos.set(0);
        }
        long memPos = -1;
        while (blockPos.get() < blocks.size() && (memPos = localBlock().allocate(cap)) == -1){
            stables.put(localBlock().getId(), localBlock());
            blockPos.set(blockPos.get() + 1);
        }
        if (memPos == -1){
            return null;
        }

        long oldPos = this.aofPos.get();
        long newPos = aofPosition + cap;
        while (newPos > oldPos && ! this.aofPos.compareAndSet(oldPos, newPos)){
            oldPos = this.aofPos.get();
        }
        return new PMem(localBlock(), memPos, cap);
    }

    public Data allocateActive(int capacity){
        if (heap == null){
            return new Dram(capacity);
        }
        long position = active.allocate(capacity);
        if (position == -1){
            return new Dram(capacity);
        }
        return new PMem(active, position, capacity);
    }

}
