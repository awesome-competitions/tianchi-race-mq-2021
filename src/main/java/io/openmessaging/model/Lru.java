package io.openmessaging.model;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

public class Lru<K,V>{

    private HashMap<K, V> map;

    private int limit;

    private ReentrantLock lock;

    public Lru(int core, Consumer<V> consumer){
        this.limit = core;
        this.lock = new ReentrantLock();
        map = new LinkedHashMap<K, V>(core,0.75f, true){
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                boolean removed = size() > limit;
                if (removed){
                    consumer.accept(eldest.getValue());
                }
                return removed;
            }

        };
    }

    public V computeIfAbsent(K k, Function<? super K, ? extends V> mappingFunction){
        try{
            lock.lock();
            return map.computeIfAbsent(k, mappingFunction);
        }finally {
            lock.unlock();
        }
    }

    public int size(){
        return map.size();
    }

    public int getLimit() {
        return limit;
    }

}
