package io.openmessaging.mq;

import io.openmessaging.consts.Const;

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
    public static long extSize = 0;


    public static String information(){
        return "queueCount: " + queueCount +
                ", appendSize: " + appendSize +
                ", appendCount: " + appendCount +
                ", readMemCount: " + readMemCount +
                ", writeMemCount: " + writeMemCount +
                ", writeDramCount: " + writeDramCount +
                ", missingDramSize: " + missingDramSize +
                ", writeExtraDramCount: " + writeExtraDramCount +
                ", readSSDCount: " + readSSDCount +
                ", aep tasks: " + Mq.AEP_TASKS.size() +
                ", heap max: " + runtime.maxMemory() / Const.M +
                ", heap used: " + (runtime.totalMemory() - runtime.freeMemory()) / Const.M +
                ", extSize: " + extSize
                ;
    }
}
