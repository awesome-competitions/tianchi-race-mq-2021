package io.openmessaging;

import io.openmessaging.utils.FileUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileChannelStorage {

//    private static final String DATA_PATH = "/data";
    private static final String DATA_PATH = "D:\\test";

    private static final Map<String, Queue> QUEUES = new ConcurrentHashMap<>();

    public static Queue getInstance(String key){
        return QUEUES.computeIfAbsent(key, Queue::new);
    }

    public static void init() throws IOException {
        FileUtil.createIfAbsent(DATA_PATH, true);
    }

    public static class Queue{
        private final FileChannel writer;
        private int offset;
        private final File file;
        private final Map<Long, BufferedInputStream> offsets;

        public Queue(String key)  {
            try {
                String filePath = DATA_PATH + File.separator + key;
                this.file = FileUtil.createIfAbsent(filePath, false);
                this.writer = new RandomAccessFile(filePath, "rw").getChannel();
                this.offsets = new ConcurrentHashMap<>();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public int write(ByteBuffer byteBuffer) {
            try {
                this.writer.write(byteBuffer);
//                this.writer.force(true);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return this.offset ++;
        }

        public synchronized BufferedInputStream getOffsetAndDelete(long offset){
            BufferedInputStream reader = offsets.get(offset);
            if (reader != null){
                offsets.remove(offset);
            }
            return reader;
        }

        public byte[][] read(long offset, int num) throws IOException {
            byte[][] data = new byte[num][];
            BufferedInputStream reader = getOffsetAndDelete(offset);
            Map<Integer, ByteBuffer> results = new LinkedHashMap<>();
            FileInputStream reader = getOffsetAndDelete(offset);
            if (reader == null){
                reader = new BufferedInputStream(new FileInputStream(file));
                skipN(reader, offset - 1);
            }
            for (int i = 0; i < num; i ++){
                data[i] = readLine(reader);
            }
            offsets.put(offset + num, reader);
            return data;
        }

        public void skipN(InputStream os, long n){
            try {
                for(int i = 0; i < n; i ++){
                    int t;
                    while((t = os.read()) != '\n' && t != -1){ }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public byte[] readLine(InputStream os) throws IOException {
            int i;
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            while((i = os.read()) != '\n' && i != -1){
                bytes.write(i);
            }
            return bytes.toByteArray();
        }

    }

}
