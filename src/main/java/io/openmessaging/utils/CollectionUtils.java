package io.openmessaging.utils;

import java.util.Collection;
import java.util.List;

public class CollectionUtils {

    public static boolean isEmpty(Collection<?> collection){
        return collection == null || collection.isEmpty();
    }

    public static boolean isNotEmpty(Collection<?> collection){
        return ! isEmpty(collection);
    }

    public static <T> T lastOf(List<T> list){
        if (isEmpty(list)){
            return null;
        }
        return list.get(list.size() - 1);
    }
}
