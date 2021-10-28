package io.openmessaging.impl;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;

public class ResultMap implements java.util.Map<Integer, ByteBuffer> {

    private final ByteBuffer[] buffers = new ByteBuffer[Const.MAX_FETCH_NUM];

    private int maxIndex;

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        return true;
    }

    @Override
    public boolean containsValue(Object value) {
        return true;
    }

    @Override
    public ByteBuffer get(Object key) {
        int index = (int) key;
        if (index > maxIndex){
            return null;
        }
        return buffers[index];
    }

    @Override
    public ByteBuffer put(Integer key, ByteBuffer value) {
        buffers[key] = value;
        return value;
    }

    @Override
    public ByteBuffer remove(Object key) {
        buffers[(int) key] = null;
        return null;
    }

    @Override
    public void putAll(java.util.Map<? extends Integer, ? extends ByteBuffer> m) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Set<Integer> keySet() {
        return null;
    }

    @Override
    public Collection<ByteBuffer> values() {
        return null;
    }

    @Override
    public Set<Entry<Integer, ByteBuffer>> entrySet() {
        return null;
    }

    public void setMaxIndex(int maxIndex){
        this.maxIndex = maxIndex;
    }

}
