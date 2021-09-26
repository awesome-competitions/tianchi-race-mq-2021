package io.openmessaging.mq;

import java.util.List;

public class Queue {

    private int tid;

    private int qid;

    private long size;

    private long offset;

    private long startOffset;

    private long endOffset;

    private List<Data> records;

    private List<Block> blocks;

    public Queue(int tid) {
        this.tid = tid;
        this.offset = -1;
    }

    public long applyOffset(){
        return ++offset;
    }

}
