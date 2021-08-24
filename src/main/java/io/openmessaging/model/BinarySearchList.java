package io.openmessaging.model;


import java.util.ArrayList;
import java.util.List;

public class BinarySearchList<T> {

    private List<T> list;

    public BinarySearchList(){
        this.list = new ArrayList<>();
    }

    public void add(T t){
        this.list.add(t);
    }

    public int size(){
        return list.size();
    }

    public T search(){
        return null;
    }

}
