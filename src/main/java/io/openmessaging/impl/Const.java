package io.openmessaging.impl;

public interface Const {
    long K = 1024;
    long M = 1024 * K;
    long G = 1024 * M;
    long K_4 = 4 * K;

    long SECOND = 1000;
    long MINUTE = SECOND * 60;

    // 协议头9B
    int PROTOCOL_HEADER_SIZE = 9;
    // 协议体17KB
    int PROTOCOL_DATA_MAX_SIZE = (int) (Const.K * 17);
    // 协议17KB + 9B
    int PROTOCOL_MAX_SIZE = PROTOCOL_HEADER_SIZE + PROTOCOL_DATA_MAX_SIZE;
    // 刷盘缓冲区256KB
    int AOF_FLUSHED_BUFFER_SIZE = (int) (Const.K * 256);
    // getRange最大fetchNum 100
    int MAX_FETCH_NUM = 100;
}
