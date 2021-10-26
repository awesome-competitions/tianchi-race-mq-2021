package io.openmessaging.impl;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

public class Utils {

    public static void recycleByteBuffer(ByteBuffer bf) {
        Cleaner var1 = ((DirectBuffer)bf).cleaner();
        if (var1 != null) {
            var1.clean();
        }
    }


}
