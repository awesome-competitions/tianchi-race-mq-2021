package io.openmessaging.model;

import java.util.Objects;

public class Tetrad<K1, K2, K3, K4> {

    private final K1 k1;

    private final K2 k2;

    private final K3 k3;

    private final K4 k4;

    public Tetrad(K1 k1, K2 k2, K3 k3, K4 k4) {
        this.k1 = k1;
        this.k2 = k2;
        this.k3 = k3;
        this.k4 = k4;
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

    public K4 getK4() {
        return k4;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tetrad<?, ?, ?, ?> tetrad = (Tetrad<?, ?, ?, ?>) o;
        return Objects.equals(k1, tetrad.k1) && Objects.equals(k2, tetrad.k2) && Objects.equals(k3, tetrad.k3) && Objects.equals(k4, tetrad.k4);
    }

    @Override
    public int hashCode() {
        return Objects.hash(k1, k2, k3, k4);
    }
}
