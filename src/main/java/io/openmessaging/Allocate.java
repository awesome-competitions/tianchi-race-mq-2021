package io.openmessaging;

public class Allocate {

    private long offset;

    private long position;

    private long capacity;

    public Allocate(long offset, long position, long capacity) {
        this.offset = offset;
        this.position = position;
        this.capacity = capacity;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
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
