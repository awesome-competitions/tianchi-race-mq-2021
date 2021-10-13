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

    public Block getBlock() {
        return block;
    }

    public Data allocate(int cap){
        Data data = Threads.get().allocatePMem(cap);
        if (data == null){
            Monitor.missingIdleCount ++;
        }else{
            Monitor.allocateIdleCount ++;
        }
        return data;
    }


}
