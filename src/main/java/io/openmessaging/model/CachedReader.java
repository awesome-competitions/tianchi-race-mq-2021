package io.openmessaging.model;

import java.nio.MappedByteBuffer;

public class CachedReader {

    private Allocate allocate;

    private MappedByteBuffer mappedByteBuffer;

    public CachedReader(Allocate allocate, MappedByteBuffer mappedByteBuffer) {
        this.allocate = allocate;
        this.mappedByteBuffer = mappedByteBuffer;
    }

    public Allocate getAllocate() {
        return allocate;
    }

    public void setAllocate(Allocate allocate) {
        this.allocate = allocate;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public void setMappedByteBuffer(MappedByteBuffer mappedByteBuffer) {
        this.mappedByteBuffer = mappedByteBuffer;
    }
}
