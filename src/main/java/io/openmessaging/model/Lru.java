package io.openmessaging.model;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Lru<K,V>{

    private HashMap<K, V> map;

    private int limit;

    private ReentrantLock lock;

    public Lru(int core){
        this.limit = core;
        this.lock = new ReentrantLock();
        map = new LinkedHashMap<K, V>(){
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return this.size() > limit && super.removeEldestEntry(eldest);
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

    public void remove(K k){
        try{
            lock.lock();
            map.remove(k);
        }finally {
            lock.unlock();
        }
    }

    public V get(K k){
        return map.get(k);
    }

}
