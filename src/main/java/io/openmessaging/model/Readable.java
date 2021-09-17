package io.openmessaging.model;

public class Readable {

    private Segment segment;

    private long startOffset;

    private long endOffset;

    public Readable(Segment segment, long startOffset, long endOffset) {
        this.segment = segment;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public Segment getSegment() {
        return segment;
    }

    public void setSegment(Segment segment) {
        this.segment = segment;
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

    @Override
    public String toString() {
        return "Readable{" +
                "segment=" + segment +
                ", startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                '}';
    }
}
