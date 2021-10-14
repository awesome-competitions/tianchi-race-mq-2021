package io.openmessaging.mq;

import java.nio.ByteBuffer;

public class AepTask {

    private long position;

    private Block block;

    private ByteBuffer data;

    public AepTask(long position, Block block, ByteBuffer data) {
        this.position = position;
        this.block = block;
        this.data = data;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public Block getBlock() {
        return block;
    }

    public void setBlock(Block block) {
        this.block = block;
    }

    public ByteBuffer getData() {
        return data;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }
}
