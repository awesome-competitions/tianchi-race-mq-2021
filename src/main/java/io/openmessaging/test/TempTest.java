package io.openmessaging.test;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class TempTest {

    public static void main(String[] args) {
//        byte[] bytes = new byte[]{-69, 111};
//        short s = (short)((bytes[0] << 8) | (bytes[1] & 0xff));
//        System.out.println(s);

        ByteBuffer byteBuffer = ByteBuffer.allocate(2);
        byteBuffer.putShort(Short.MAX_VALUE);
        byte[] bytes = new byte[2];
        byteBuffer.flip();
        byteBuffer.get(bytes);
        System.out.println(Arrays.toString(bytes));
    }
}
