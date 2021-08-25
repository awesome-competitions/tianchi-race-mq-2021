package io.openmessaging.model;

import java.util.Objects;

public class Tuple<K, V> {

    private K k;

    private V v;

    public Tuple(K k, V v) {
        this.k = k;
        this.v = v;
    }

    public K getK() {
        return k;
    }

    public void setK(K k) {
        this.k = k;
    }

    public V getV() {
        return v;
    }

    public void setV(V v) {
        this.v = v;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple<?, ?> tuple = (Tuple<?, ?>) o;
        return Objects.equals(k, tuple.k) && Objects.equals(v, tuple.v);
    }

    @Override
    public int hashCode() {
        return Objects.hash(k, v);
    }
}
