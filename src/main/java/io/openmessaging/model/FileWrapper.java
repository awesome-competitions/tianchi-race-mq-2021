package io.openmessaging.model;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileWrapper {

    private final RandomAccessFile file;

    private final FileChannel channel;

    public FileWrapper(RandomAccessFile file) {
        this.file = file;
        this.channel = file.getChannel();
    }

    public synchronized int write(long position, ByteBuffer src) throws IOException {
        channel.position(position);
        return write(src);
    }

    public synchronized int write(ByteBuffer src) throws IOException {
        int pos = channel.write(src);
        channel.force(true);
        return pos;
    }

    public synchronized int read(long position, ByteBuffer dst) throws IOException {
        channel.position(position);
        return channel.read(dst);
    }

    public FileChannel getChannel(){
        return channel;
    }

    public RandomAccessFile getFile() {
        return file;
    }

}
