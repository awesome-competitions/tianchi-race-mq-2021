package io.openmessaging;

public class Allocate {

    private long start;

    private long end;

    private long position;

    private long capacity;

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
}
