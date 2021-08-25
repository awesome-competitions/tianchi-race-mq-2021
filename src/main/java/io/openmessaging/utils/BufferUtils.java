package io.openmessaging.utils;

import io.openmessaging.model.CachedReader;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileDescriptor;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;

public class BufferUtils {

    public static void clean(MappedByteBuffer mappedByteBuffer) {
        Cleaner var1 = ((DirectBuffer)mappedByteBuffer).cleaner();
        if (var1 != null) {
            var1.clean();
        }
    }

    public static FileDescriptor getFd(MappedByteBuffer mappedByteBuffer){
        try {
            Field field = MappedByteBuffer.class.getDeclaredField("fd");
            field.setAccessible(true);
            Object obj = field.get(mappedByteBuffer);
            return (FileDescriptor) obj;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static MappedByteBuffer newInstance(int mark, int pos, int lim, int cap, FileDescriptor fd){
        try {
            MappedByteBuffer.class.getDeclaredConstructor(int.class, int.class, int.class, int.class, FileDescriptor.class);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

}
