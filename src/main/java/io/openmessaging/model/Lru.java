package io.openmessaging.model;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

public class Lru<K>{

    private HashMap<K, Object> map;

    private int limit;

    private ReentrantLock lock;

    private Consumer<K> consumer;

    public Lru(int core, Consumer<K> consumer){
        this.limit = core;
        this.lock = new ReentrantLock();
        this.consumer = consumer;
        map = new LinkedHashMap<K, Object>(core,0.75f, true){
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, Object> eldest) {
                boolean removed = size() > limit;
                if (removed){
                    consumer.accept(eldest.getKey());
                }
                return removed;
            }

        };
    }

    public K add(K k){
        try{
            lock.lock();
            map.put(k, null);
            return k;
        }finally {
            lock.unlock();
        }
    }

    public K remove(K k){
        try{
            lock.lock();
            map.remove(k);
            return k;
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
