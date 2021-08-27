package io.openmessaging.test;

import io.openmessaging.MessageQueue;
import io.openmessaging.impl.MessageQueueImpl;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Scanner;

public class TestCommand {

    public static void main(String[] args) {
        MessageQueueImpl messageQueue = new MessageQueueImpl();
        messageQueue.loadDB();
        Scanner in = new Scanner(System.in);
        String topic = "test_command";
        while(true){
            String line = in.nextLine();
            String[] commands = line.split(" ");
            long start = System.currentTimeMillis();
            if (commands[0].equals("set")){
                long offset = messageQueue.append(topic, Integer.parseInt(commands[1]), ByteBuffer.wrap(commands[2].getBytes()));
                System.out.print("new offset " + offset);
            }else{
                Map<Integer, ByteBuffer> result = messageQueue.getRange(topic, Integer.parseInt(commands[1]), Integer.parseInt(commands[2]), 1);
                System.out.print("result " + (result == null ? "null" : new String(result.get(0).array())));
            }
            long end = System.currentTimeMillis();
            System.out.println(", " + commands[0] + " spend " + (end - start) + "ms");
        }


    }

}
