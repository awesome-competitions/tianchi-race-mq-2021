package io.openmessaging.mq;

import io.openmessaging.utils.OSUtils;

public class Monitor {

    public static long queueCount = 0;
    public static long appendSize = 0;
    public static long appendCount = 0;
    public static long readMemCount = 0;
    public static long writeMemCount = 0;
    public static long readSSDCount = 0;
    public static long swapSSDToPmemCount = 0;
    public static long allocateIdleCount = 0;
    public static long missingIdleCount = 0;


    public static String information(){
        OSUtils.memoryUsage();
        return "queueCount: " + queueCount +
                ", appendSize: " + appendSize +
                ", appendCount: " + appendCount +
                ", readMemCount: " + readMemCount +
                ", writeMemCount: " + writeMemCount +
                ", readSSDCount: " + readSSDCount +
                ", swapSSDToPmemCount: " + swapSSDToPmemCount +
                ", allocateIdleCount: " + allocateIdleCount +
                ", missingIdleCount: " + missingIdleCount
                ;
    }
}
