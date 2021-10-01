package io.openmessaging.mq;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SSD extends Data{

    private final FileWrapper fw;

    private final List<Record> records;

    private long aos;

    private final static Map<Long, Data> EMPTY = new HashMap<>();

    public SSD(FileWrapper fw, long position, int capacity) {
        super(capacity);
        this.fw = fw;
        this.position = position;
        this.aos = position;
        this.records = new ArrayList<>();
    }

    @Override
    public ByteBuffer get() {
        throw new UnsupportedOperationException("ssd block not supported set");
    }

    @Override
    public void set(ByteBuffer buffer) {
        throw new UnsupportedOperationException("ssd block not supported set");
    }

    public Map<Long, Data> load() {
        if (records.isEmpty()){
            return EMPTY;
        }
        Monitor.readSSDBlockCount ++;
        ByteBuffer buffer = ByteBuffer.allocate((int) (aos - position));
        try {
            fw.read(position, buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        buffer.flip();
        Map<Long, Data> result = new HashMap<>();
        for (Record record: records){
            byte[] bytes = new byte[(int) record.getCapacity()];
            buffer.get(bytes);
            result.put(record.getOffset(), new Dram(ByteBuffer.wrap(bytes)));
        }
        return result;
    }

    public void write(long offset, ByteBuffer buffer){
        try {
            Monitor.writeSSDBlockCount ++;
            fw.write(aos, buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        aos += buffer.limit();
        this.records.add(new Record(offset, buffer.limit()));
    }

    public boolean writable(ByteBuffer buffer){
        return capacity - (aos - position) >= buffer.limit();
    }

    public FileWrapper getFw() {
        return fw;
    }

    @Override
    public void clear() {}
}
