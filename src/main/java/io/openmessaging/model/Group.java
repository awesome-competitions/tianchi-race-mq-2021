package io.openmessaging.model;

import java.util.concurrent.atomic.AtomicInteger;

public class Group {

    private FileWrapper db;

    private FileWrapper idx;

    private AtomicInteger pageOffset;

    public Group(FileWrapper db, FileWrapper idx) {
        this.db = db;
        this.idx = idx;
        this.pageOffset = new AtomicInteger(0);
    }

    public int getAndIncrementOffset(){
        return pageOffset.getAndIncrement();
    }

    public FileWrapper getDb() {
        return db;
    }

    public FileWrapper getIdx() {
        return idx;
    }
}
