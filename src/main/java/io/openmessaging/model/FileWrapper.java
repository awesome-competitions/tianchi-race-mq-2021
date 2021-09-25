package io.openmessaging.model;

import sun.nio.ch.FileChannelImpl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class FileWrapper {

    static final OpenOption[] options = {
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE
    };

    private final FileChannel channel;

    public FileWrapper(RandomAccessFile file) throws IOException {
        this.channel = file.getChannel();
    }

    public FileChannel getFileChannel(){
        return this.channel;
    }

    private void position(long pos) throws IOException {
        if (channel.position() != pos){
            channel.position(pos);
        }
    }

    public synchronized void write(long position, ByteBuffer src) throws IOException {
        position(position);
        channel.write(src);
    }

    public synchronized void write(long position, List<ByteBuffer> src) throws IOException {
        position(position);
        channel.write(src.toArray(new ByteBuffer[]{}));
    }

    public synchronized long write(ByteBuffer[] buffers) throws IOException {
        long pos = channel.position();
        channel.write(buffers);
        return pos;
    }

    public synchronized int read(long position, ByteBuffer dst) throws IOException {
        position(position);
        return channel.read(dst);
    }

    public synchronized int read(ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }

    public FileChannel getChannel(){
        return channel;
    }

}
