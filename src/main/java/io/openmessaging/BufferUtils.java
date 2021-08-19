package io.openmessaging;

import sun.nio.ch.FileChannelImpl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

public class BufferUtils {

    public static void unmap(MappedByteBuffer mappedByteBuffer) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method m = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
        m.setAccessible(true);
        m.invoke(FileChannelImpl.class, mappedByteBuffer);
    }

}
