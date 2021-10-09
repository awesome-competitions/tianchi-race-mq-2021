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



    private final static int BATCH = 1000;
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
        MessageQueue mq = getMq(1);
        String topic = "topic1";

        for (int i = 0; i < BATCH; i ++){
            mq.append(topic, 1, ByteBuffer.wrap(("" + i).getBytes()));
        }

        for (int i = 0; i < BATCH; i ++){
            Map<Integer, ByteBuffer> data = mq.getRange(topic, 1, i, 1);
            byte[] bytes = new byte[data.get(0).limit()];
            data.get(0).get(bytes);
            if (!Arrays.equals(bytes, ("" + i).getBytes())){
                System.out.println("topic " + topic + ", queue " + 1 + " read fail at " + i);
                break;
            }
        }

        mq = getMq(1);
        for (int i = 0; i < BATCH; i ++){
            Map<Integer, ByteBuffer> data = mq.getRange(topic, 1, i, 1);
            byte[] bytes = new byte[data.get(0).limit()];
            data.get(0).get(bytes);
            if (!Arrays.equals(bytes, ("" + i).getBytes())){
                System.out.println("topic " + topic + ", queue " + 1 + " read fail at " + i);
                break;
            }
        }

//        long offset = mMapMessageQueueNew.append("topic3", 1, ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));
//        Map<Integer, ByteBuffer> values = mMapMessageQueueNew.getRange("topic3", 1, offset, 1);
//        System.out.println(offset + ":" + new String(values.get(0).array()));
//
//        POOLS.shutdown();
    }


}
