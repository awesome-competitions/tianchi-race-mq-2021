package io.openmessaging.mq;

public class Monitor {

    public static Runtime runtime = Runtime.getRuntime();

    public static long queueCount = 0;
    public static long appendSize = 0;
    public static long appendCount = 0;
    public static long readMemCount = 0;
    public static long writeMemCount = 0;
    public static long writeDramCount = 0;
    public static long writeExtraDramCount = 0;
    public static long readSSDCount = 0;
    public static long extSize = 0;


    public static String information(){
        return "queueCount: " + queueCount +
                ", appendSize: " + appendSize +
                ", appendCount: " + appendCount +
                ", readMemCount: " + readMemCount +
                ", writeMemCount: " + writeMemCount +
                ", writeDramCount: " + writeDramCount +
                ", writeExtraDramCount: " + writeExtraDramCount +
                ", readSSDCount: " + readSSDCount +
                ", aep tasks: " + Mq.AEP_TASKS.size() +
                ", get tasks: " + Queue.TPE.getActiveCount() +
                ", heap free: " + runtime.freeMemory() +
                ", heap used: " + (runtime.totalMemory() - runtime.freeMemory()) +
                ", extSize: " + extSize
                ;
    }
}
