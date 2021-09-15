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

public class FileWrapper {

    static final OpenOption[] options = {
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE_NEW
    };

    private final FileChannel channel;

    public FileWrapper(Path path) throws IOException {
        this.channel = FileChannelImpl.open(path, options);
    }

    private void position(long pos) throws IOException {
        if (channel.position() != pos){
            channel.position(pos);
        }
    }

    public synchronized int write(long position, ByteBuffer src) throws IOException {
        position(position);
        return write(src);
    }

    public synchronized int write(ByteBuffer src) throws IOException {
        int pos = channel.write(src);
        channel.force(false);
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
