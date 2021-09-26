package io.openmessaging.cache;

import com.intel.pmem.llpl.AnyMemoryBlock;
import io.openmessaging.model.Aof;
import io.openmessaging.model.FileWrapper;
import io.openmessaging.utils.CollectionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SSD extends Storage {

    private long pos;

    private long cap;

    private long aos;

    private FileWrapper fw;

    private List<Long> positions;

    private int idx;

    public SSD(long pos, long cap, FileWrapper fw) {
        this.pos = pos;
        this.cap = cap;
        this.aos = pos;
        this.fw = fw;
        reset(0, null, 0);
    }

    @Override
    public List<ByteBuffer> read(long startOffset, long endOffset) {
        throw new UnsupportedOperationException("ssd unsupport read");
    }

    @Override
    public List<ByteBuffer> load() {
        if (cap == 0){
            return new ArrayList<>();
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) cap);
        try {
            fw.read(pos, buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        buffer.flip();
        int startIndex = 0;
        List<ByteBuffer> data = new ArrayList<>();
        while (startIndex < positions.size()){
            Long size = positions.get(startIndex);
            byte[] bytes = new byte[size.intValue()];
            buffer.get(bytes);
            data.add(ByteBuffer.wrap(bytes));
            startIndex ++;
        }
        return data;
    }

    @Override
    public void write(ByteBuffer byteBuffer) {
        throw new UnsupportedOperationException("ssd unsupport read");
    }

    @Override
    public void reset(int idx, List<ByteBuffer> buffers, long beginOffset) {
        cap = 0;
        this.idx = idx;
        if (positions == null){
            positions = new ArrayList<>();
        }
        positions.clear();
        if (CollectionUtils.isNotEmpty(buffers)) {
            for (ByteBuffer buffer: buffers) {
                positions.add((long) buffer.capacity());
                cap += buffer.capacity();
            }
            try {
                this.pos = fw.write(buffers.toArray(Aof.EMPTY));
                this.aos = pos + cap;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public long getIdx() {
        return idx;
    }

    @Override
    public void clean() {

    }
}
