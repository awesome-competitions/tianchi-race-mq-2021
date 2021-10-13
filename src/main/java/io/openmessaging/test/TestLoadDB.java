package io.openmessaging.test;

import io.openmessaging.MessageQueue;
import io.openmessaging.consts.Const;
import io.openmessaging.impl.MessageQueueImpl;
import io.openmessaging.model.Config;
import io.openmessaging.mq.Mq;
import io.openmessaging.utils.ArrayUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class TestLoadDB {



    private final static int BATCH = 100000;
    private final static int PARALLEL_SIZE = 1;
    private final static String DIR = "D://test//nio//";
    private final static String HEAP_DIR = null;

    public static Mq getMq(int count) throws IOException {
        return new Mq(new io.openmessaging.mq.Config(DIR, null, 0, count, (int) (Const.K * 512), 0));
    }

    public static void cleanDB(){
        File root = new File(DIR);
        if (root.exists() && root.isDirectory()){
            if (ArrayUtils.isEmpty(root.listFiles())) return;
            for (File file: Objects.requireNonNull(root.listFiles())){
                if (file.exists() && ! file.isDirectory() && file.delete()){ }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        cleanDB();
        final MessageQueue mq = getMq(1);
        String topic1 = "topic1";
        String topic2 = "topic2";
        String topic3 = "topic3";


        CountDownLatch cdl = new CountDownLatch(3);
        new Thread(()->{
            write(mq, topic1);
            cdl.countDown();
        }).start();;
        new Thread(()->{
            write(mq, topic2);
            cdl.countDown();
        }).start();;
        new Thread(()->{
            write(mq, topic3);
            cdl.countDown();
        }).start();;
        cdl.await();


        final MessageQueue mq1 = getMq(1);
        read(mq1, topic1);
        read(mq1, topic2);
        read(mq1, topic3);

    }

    public static void write(MessageQueue mq, String topic){
        for (int i = 0; i < BATCH; i ++){
            mq.append(topic, i % 2, ByteBuffer.wrap(("" + i).getBytes()));
        }
    }

    public static void read(MessageQueue mq, String topic){
        for (int i = 0; i < BATCH; i ++){
            Map<Integer, ByteBuffer> data = mq.getRange(topic, i % 2, i / 2, 1);
            byte[] bytes = new byte[data.get(0).limit()];
            data.get(0).get(bytes);
            if (!Arrays.equals(bytes, ("" + i).getBytes())){
                System.out.println("topic " + topic + ", queue " + 1 + " read fail at " + i);
                break;
            }
        }
    }


}
