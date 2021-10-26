package io.openmessaging.mq;

public class Monitor {

    public static Runtime runtime = Runtime.getRuntime();

    public static long queueCount = 0;
    public static long appendSize = 0;
    public static long appendCount = 0;
    public static long readMemCount = 0;
    public static long writeMemCount = 0;
    public static long writeDramCount = 0;
    public static long writeDramSize = 0;
    public static long missingDramSize = 0;
    public static long writeExtraDramCount = 0;
    public static long readSSDCount = 0;
    public static long writeSSDCount = 0;
    public static long readPreSSDCount = 0;
    public static long extSize = 0;


    public static String information(){
        return "queueCount: " + queueCount +
                ", appendSize: " + appendSize +
                ", appendCount: " + appendCount +
                ", readAepCount: " + readMemCount +
                ", writeAepCount: " + writeMemCount +
                ", writeDramCount: " + writeDramCount +
                ", missingDramSize: " + missingDramSize +
                ", writeExtraDramCount: " + writeExtraDramCount +
                ", readSSDCount: " + readSSDCount +
                ", writeSSDCount: " + writeSSDCount +
                ", readPreSSDCount: " + readPreSSDCount +
                ", heap used: " + (runtime.totalMemory() - runtime.freeMemory()) / Const.M +
                ", extSize: " + extSize
                ;
    }
}
