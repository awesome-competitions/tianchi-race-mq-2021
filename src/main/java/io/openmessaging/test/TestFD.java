package io.openmessaging.test;

import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class TestFD {

    public static void main(String[] args) throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        String file = "D:\\test\\mmap\\test.txt";
        List<RandomAccessFile> files = new ArrayList<>();

        Field field = FileDescriptor.class.getDeclaredField("handle");
        field.setAccessible(true);
        int i = 0;
        while(true){
            files.add(new RandomAccessFile(file, "rw"));
            System.out.println(field.get(new RandomAccessFile(file, "rw").getFD()) + ":" + (++i));
        }
    }
}
