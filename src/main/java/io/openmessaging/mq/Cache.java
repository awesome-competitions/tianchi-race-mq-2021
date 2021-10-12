package io.openmessaging.mq;

import io.openmessaging.consts.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

public class Cache {

    private Block block;

    private final LinkedList<Data> idles1 = new LinkedList<>();
    private final LinkedList<Data> idles2 = new LinkedList<>();
    private final LinkedList<Data> idles3 = new LinkedList<>();
    private final LinkedList<Data> idles4 = new LinkedList<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(Cache.class);

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
            Data data = getIdles(cap).poll();
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
        getIdles(data.getCapacity()).add(data);
    }

    private LinkedList<Data> getIdles(int cap){
        if (cap < Const.K * 4.5){
            return idles1;
        }else if (cap < Const.K * 9){
            return idles2;
        }else if (cap < Const.K * 13.5){
            return idles3;
        }else{
            return idles4;
        }
    }

}
