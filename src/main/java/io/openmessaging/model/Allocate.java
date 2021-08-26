package io.openmessaging.model;

import io.openmessaging.utils.BufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class Allocate {

    private long start;

    private long end;

    private int index;

    private long position;

    private long capacity;

    private Allocate(){ }

    public Allocate(long start, long end, long position, long capacity) {
        this.start = start;
        this.end = end;
        this.position = position;
        this.capacity = capacity;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public long getCapacity() {
        return capacity;
    }

    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public List<ByteBuffer> load(FileChannel channel) {
        MappedByteBuffer mappedByteBuffer;
        try {
            mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, position, capacity);
            short size;
            List<ByteBuffer> records = new ArrayList<>();
            while ((size = mappedByteBuffer.getShort()) > 0){
                byte[] bytes = new byte[size];
                mappedByteBuffer.get(bytes);
                records.add(ByteBuffer.wrap(bytes));
            }
            BufferUtils.clean(mappedByteBuffer);
            return records;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
