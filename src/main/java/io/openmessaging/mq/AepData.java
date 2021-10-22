package io.openmessaging.mq;

import java.nio.ByteBuffer;
import java.util.List;

public class AepData {

    private Data data;

    private ByteBuffer byteBuffer;

    private long offset;

    private List<Data> records;

    public AepData(Data data, ByteBuffer byteBuffer, long offset, List<Data> records) {
        this.data = data;
        this.byteBuffer = byteBuffer;
        this.offset = offset;
        this.records = records;
    }

    public Data getData() {
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public void setByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public List<Data> getRecords() {
        return records;
    }

    public void setRecords(List<Data> records) {
        this.records = records;
    }
}
