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
        map = new LinkedHashMap<K, V>(){
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

    public void put(K k, V v){
        try{
            lock.lock();
            map.put(k, v);
        }finally {
            lock.unlock();
        }
    }

    public V remove(K k){
        try{
            lock.lock();
            return map.remove(k);
        }finally {
            lock.unlock();
        }
    }

    public V get(K k){
        return map.get(k);
    }

    public V computeIfAbsent(K k, Function<? super K, ? extends V> mappingFunction){
        return map.computeIfAbsent(k, mappingFunction);
    }

    public int size(){
        return map.size();
    }

}
