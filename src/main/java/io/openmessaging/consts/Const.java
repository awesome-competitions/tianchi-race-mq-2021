package io.openmessaging.consts;

public interface Const {
    String DB_NAMED_FORMAT = "%s%s_%d.db";
    String IDX_NAMED_FORMAT = "%s%s_%d.idx";

    long K = 1024;
    long M = 1024 * K;
    long G = 1024 * M;

    long SECOND = 1000;
    long MINUTE = SECOND * 60;
}
