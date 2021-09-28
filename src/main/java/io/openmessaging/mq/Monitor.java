package io.openmessaging.mq;

public class Monitor {

    public static long queueCount = 0;
    public static long appendSize = 0;
    public static long appendCount = 0;
    public static long writeDistCount = 0;
    public static long readDistCount = 0;

    public static String information(){
        return "queueCount: " + queueCount + ", appendSize: " + appendSize + ", appendCount: " + appendCount + ", writeDistCount: " + writeDistCount + ", readDistCount: " + readDistCount;
    }
}
