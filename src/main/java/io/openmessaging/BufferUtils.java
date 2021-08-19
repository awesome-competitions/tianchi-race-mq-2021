package io.openmessaging;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;
import sun.nio.ch.FileChannelImpl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

public class BufferUtils {

    public static void clean(MappedByteBuffer mappedByteBuffer) {
        Cleaner var1 = ((DirectBuffer)mappedByteBuffer).cleaner();
        if (var1 != null) {
            var1.clean();
        }
    }

}
