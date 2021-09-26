package io.openmessaging.mq;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

public class Lru<K, V> extends LinkedHashMap<K, V>{

    private final ReentrantLock lock;

    private final int core;

    public Lru(int core){
        super(core, 0.75f, true);
        this.lock = new ReentrantLock();
        this.core = core;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > core;
    }

    @Override
    public V get(Object key) {
        return super.get(key);
    }

    @Override
    public V put(K key, V value) {
        return super.put(key, value);
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        return super.computeIfAbsent(key, mappingFunction);
    }
}
