package io.openmessaging.impl;

public interface Const {
    long K = 1024;
    long M = 1024 * K;
    long G = 1024 * M;
    long K_4 = 4 * K;

    long SECOND = 1000;
    long MINUTE = SECOND * 60;

    int PROTOCOL_HEADER_SIZE = 9;
    int PROTOCOL_DATA_MAX_SIZE = (int) (Const.K * 17);
    int PROTOCOL_MAX_SIZE = PROTOCOL_HEADER_SIZE + PROTOCOL_DATA_MAX_SIZE;

    int AOF_FLUSHED_BUFFER_SIZE = (int) (Const.K * 176);
}
