package io.openmessaging.mq;

import io.openmessaging.consts.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

public class Cache {

    private Block block;

    public Cache(String heapDir, long heapSize) throws FileNotFoundException {
        if (heapDir != null){
            this.block = new Block(new FileWrapper(new RandomAccessFile(heapDir, "rw")), heapSize);
        }
    }

    public Data allocate(int cap){
        if (block == null){
            return new Dram(cap);
        }
        long memPos = block.allocate(cap);
        if (memPos == -1){
            Data data = Threads.get().getIdles(cap).poll();
            if (data == null){
                Monitor.missingIdleCount ++;
            }else{
                Monitor.allocateIdleCount ++;
            }
            return data;
        }
        return new PMem(block, memPos, cap);
    }

    public void recycle(Data data){
        data.clear();
        Threads.get().getIdles(data.getCapacity()).add(data);
    }



}
