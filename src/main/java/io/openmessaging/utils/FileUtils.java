package io.openmessaging.utils;

import java.io.File;
import java.io.IOException;

public class FileUtils {

    public static File createIfAbsent(String path, boolean isDir) throws IOException {
        File file = new File(path);
        if (file.exists()){
            return file;
        }
        if (isDir ? file.mkdirs() : file.createNewFile()) {
            return file;
        }
        return null;
    }

}
