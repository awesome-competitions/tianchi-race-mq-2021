package io.openmessaging.model;

import java.util.Objects;

public class Triple<K1, K2, K3> {

    private final K1 k1;

    private final K2 k2;

    private final K3 k3;

    public Triple(K1 k1, K2 k2, K3 k3) {
        this.k1 = k1;
        this.k2 = k2;
        this.k3 = k3;
    }

    public K1 getK1() {
        return k1;
    }

    public K2 getK2() {
        return k2;
    }

    public K3 getK3() {
        return k3;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;
        return Objects.equals(k1, triple.k1) && Objects.equals(k2, triple.k2) && Objects.equals(k3, triple.k3);
    }

    @Override
    public int hashCode() {
        return Objects.hash(k1, k2, k3);
    }
}
