package io.openmessaging.utils;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.MappedByteBuffer;

public class BufferUtils {

    public static void clean(MappedByteBuffer mappedByteBuffer) {
        Cleaner var1 = ((DirectBuffer)mappedByteBuffer).cleaner();
        if (var1 != null) {
            var1.clean();
        }
    }

}
