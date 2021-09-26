package io.openmessaging.mq;

public class Block {

    private long startOffset;

    private long endOffset;

    private long position;

    private long capacity;

    public Block(long startOffset, long endOffset, long position, long capacity) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.position = position;
        this.capacity = capacity;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
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
}
