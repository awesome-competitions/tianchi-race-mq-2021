package io.openmessaging.model;

import io.openmessaging.cache.Cache;
import io.openmessaging.utils.CollectionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Group {

    private final FileWrapper db;

    private final FileWrapper idx;

    private final AtomicInteger pageOffset;

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
