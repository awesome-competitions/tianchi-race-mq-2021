package io.openmessaging.utils;

public class ArrayUtils {

    public static <T> boolean isNotEmpty(T[] array){
        return array != null && array.length > 0;
    }
}
