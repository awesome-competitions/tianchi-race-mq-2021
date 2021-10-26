package io.openmessaging.impl;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

public class Aep {

    private boolean completed;

    private final long capacity;

    private final AtomicLong pos;

    private final FileChannel channel;

    public Aep(RandomAccessFile file, long capacity) {
        this.pos = new AtomicLong();
        this.channel = file.getChannel();
        this.capacity = capacity;
    }

    public long allocatePos(int cap){
        if (completed){
            return -1;
        }
        long newPos = pos.addAndGet(cap);
        if (newPos > this.capacity){
            pos.addAndGet(-cap);
            completed = true;
            return -1;
        }
        return newPos - cap;
    }

    public void read(long position, ByteBuffer buffer){
        try {
            channel.read(buffer, position);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(long position, ByteBuffer buffer){
        try {
            channel.write(buffer, position);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public FileChannel getChannel() {
        return channel;
    }
}
