package io.openmessaging.mq;

import java.util.ArrayList;
import java.util.List;

public class Queue {

    private int id;

    private long offset;

    private long startOffset;

    private final List<Data> records;

    private final List<Block> blocks;

    public Queue(int id) {
        this.id = id;
        this.offset = -1;
        this.records = new ArrayList<>();
        this.blocks = new ArrayList<>();
    }

    public long append(Data record){
        this.records.add(record);
        record.setOffset(++offset);
        return record.getOffset();
    }

    public List<Data> getRecords() {
        return records;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getOffset() {
        return offset;
    }

    public List<Block> getBlocks() {
        return blocks;
    }

    public int getId() {
        return id;
    }
}
