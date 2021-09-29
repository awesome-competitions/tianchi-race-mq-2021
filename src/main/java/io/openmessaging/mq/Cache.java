package io.openmessaging.mq;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.consts.Const;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Cache {

    private Heap heap;

    private Block active;

    private final List<Block> blocks = new ArrayList<>(10);

    private final ThreadLocal<Integer> blockPos = new ThreadLocal<>();

    private static final long ACTIVE_SIZE = Const.G * 3;

    private static final long BLOCK_SIZE = Const.G * 2;

    public Cache(Config config){
        if (config.getHeapDir() != null){
            this.heap = Heap.exists(config.getHeapDir()) ? Heap.openHeap(config.getHeapDir()) : Heap.createHeap(config.getHeapDir(), config.getHeapSize());
            this.active = applyBlock(ACTIVE_SIZE);
            this.blocks.add(applyBlock(BLOCK_SIZE));
            Thread producer = new Thread(() -> {
                for (int i = 0; i < 24; i ++){
                    this.blocks.add(applyBlock(BLOCK_SIZE));
                }
            });
            producer.setDaemon(true);
            producer.start();
        }

    }

    public Block applyBlock(long size){
        return new Block(heap.allocateCompactMemoryBlock(size), size);
    }

    public Data apply(int capacity){
        if (heap == null){
            return new Dram(capacity);
        }
        if (blockPos.get() == null){
            blockPos.set(0);
        }
        long position = -1;
        while (blockPos.get() < blocks.size() && (position = blocks.get(blockPos.get()).allocate(capacity)) == -1){
            blockPos.set(blockPos.get() + 1);
        }
        if (position == -1){
            return null;
        }
        return new PMem(blocks.get(blockPos.get()).getBlock(), position, capacity);
    }

    public Data applyActive(int capacity){
        if (heap == null){
            return new Dram(capacity);
        }
        long position = active.allocate(capacity);
        if (position == -1){
            return new Dram(capacity);
        }
        return new PMem(active.getBlock(), position, capacity);
    }

}
