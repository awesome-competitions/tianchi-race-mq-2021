package io.openmessaging.mq;

import com.intel.pmem.llpl.AnyMemoryBlock;
import com.intel.pmem.llpl.Heap;
import io.openmessaging.MessageQueue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Mq {

    private Heap heap;

    private final Config config;

    private final Map<Key, Data> records;

    private final LinkedBlockingQueue<Key> keys;

    private final Map<String, Map<Integer, AtomicLong>> offsets;

    private final Barrier barrier;

    private final FileWrapper aof;

    private final FileWrapper tpf;

    private final AtomicLong size;

    private final ReadWriteLock lock;

    public Mq(Config config) throws FileNotFoundException {
        this.config = config;
        this.records = new ConcurrentHashMap<>();
        this.offsets = new ConcurrentHashMap<>();
        this.keys = new LinkedBlockingQueue<>();
        this.aof = new FileWrapper(new RandomAccessFile(config.getDataDir() + "aof", "rw"));
        this.tpf = new FileWrapper(new RandomAccessFile(config.getDataDir() + "tpf", "rw"));
        this.barrier = new Barrier(config.getMaxCount(), this.aof);
        this.size = new AtomicLong();
        this.lock = new ReentrantReadWriteLock();
        if (config.getHeapDir() != null){
            this.heap = Heap.exists(config.getHeapDir()) ? Heap.openHeap(config.getHeapDir()) : Heap.createHeap(config.getHeapDir(), config.getHeapSize());
        }
    }

    Data applyBlock( ByteBuffer buffer){
        AnyMemoryBlock block = heap.allocateMemoryBlock(buffer.capacity());
        byte[] bytes = new byte[buffer.capacity()];
        buffer.get(bytes);
        block.copyFromArray(bytes, 0, 0, bytes.length);
        return new PMem(block);
    }

    Data applyData(ByteBuffer buffer){
        return heap == null ? new Dram(buffer) : applyBlock(buffer);
    }

    void append(Data data) throws IOException {
        keys.add(data.getKey());
        records.put(data.getKey(), data);
        size.addAndGet(data.size());
        if (size.get() > config.getCacheMaxSize()){
            lock.writeLock().lock();
            if (size.get() > config.getCacheMaxSize()){
                clear();
            }
            lock.writeLock().unlock();
        }
    }

    void clear() throws IOException {
        long size = 0;
        Map<String, Map<Integer, List<Data>>> clears = new HashMap<>();
        while(size < config.getCacheClearSize()){
            Key key = keys.poll();
            if (key == null){
                break;
            }
            Data data = records.remove(key);
            if (data == null) {
                continue;
            }
            size += data.size();
            clears.computeIfAbsent(key.getTopic(), k -> new HashMap<>())
                .computeIfAbsent(key.getQueueId(), k -> new ArrayList<>())
                .add(data);
        }

        for (Map.Entry<String, Map<Integer, List<Data>>> clear: clears.entrySet()){
            String topic = clear.getKey();
            for (Map.Entry<Integer, List<Data>> entry: clear.getValue().entrySet()){
                int queueId = entry.getKey();
                List<Data> list = entry.getValue();

                long startOffset = list.get(0).getKey().getOffset();
                long endOffset = list.get(list.size() - 1).getKey().getOffset();

                List<ByteBuffer> buffers = new ArrayList<>(list.size());
                long capacity = 0;
                List<Long > sizes = new ArrayList<>();
                for (Data data: list){
                    capacity += data.size();
                    buffers.add(data.get());
                    sizes.add(data.size());
                    data.clear();
                }
                long position = tpf.write(buffers.toArray(Barrier.EMPTY));
                SSD ssd = new SSD(startOffset, endOffset, position, capacity, sizes);
                ssd.setKey(new Key(topic, queueId, -1L));
                for (long i = startOffset; i <= endOffset; i ++){
                    records.put(new Key(topic, queueId, i), ssd);
                }
            }
        }
    }

    long nextOffset(String topic, int queueId){
        return offsets.computeIfAbsent(topic, k -> new HashMap<>()).computeIfAbsent(queueId, k -> new AtomicLong(-1)).addAndGet(1);
    }

    public long append(String topic, int queueId, ByteBuffer buffer) throws IOException {
        long offset = nextOffset(topic, queueId);
        Data data = applyData(buffer);
        data.setKey(new Key(topic, queueId, offset));
        append(data);
        barrier.write(buffer);
        barrier.await(10, TimeUnit.SECONDS);
        return offset;
    }

    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) throws IOException {
        lock.readLock().lock();
        long startOffset = offset;
        long endOffset = startOffset + fetchNum - 1;
        Map<Integer, ByteBuffer> results = new HashMap<>();
        for (;startOffset >= endOffset; startOffset++){
            Data data = records.remove(new Key(topic, queueId, startOffset));
            if (data == null){
                break;
            }
            if (data instanceof SSD){
                SSD ssd = (SSD) data;
                List<Data> list = ssd.load(startOffset, heap, tpf);
                long tempOffset = startOffset;
                for (Data d: list){
                    records.put(new Key(topic, queueId, tempOffset), d);
                    tempOffset ++;
                }
                data = list.get(0);
            }
            results.put((int) (startOffset - offset), data.get());
        }
        lock.readLock().unlock();
        return results;
    }



}
