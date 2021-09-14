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
//        channel.force(false);
        return pos;
    }

    public synchronized int read(long position, ByteBuffer dst) throws IOException {
        position(position);
        return channel.read(dst);
    }

    public FileChannel getChannel(){
        return channel;
    }

    public RandomAccessFile getFile() {
        return file;
    }

}
