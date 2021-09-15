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

    public void initQueues(Topic topic) throws IOException {
        ByteBuffer index = ByteBuffer.allocate(26);

        Set<Short> queueIds = new HashSet<>();
        while (idx.read(index) > 0){
            pageOffset.incrementAndGet();
            index.flip();

            short queueId = index.getShort();
            long start = index.getLong();
            long pos = index.getLong();
            long capacity = index.getLong();
            queueIds.add(queueId);
            Queue queue = topic.getQueue(queueId);
            queue.addSegment(new Segment(start, start, pos, capacity));

            index.clear();
        }

        if (CollectionUtils.isNotEmpty(queueIds)){
            for (short queueId: queueIds){
                Queue queue = topic.getQueue(queueId);
                queue.getHead().reset(db);
            }
        }
    }
}
