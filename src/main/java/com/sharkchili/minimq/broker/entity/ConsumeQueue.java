package com.sharkchili.minimq.broker.entity;

import lombok.Data;

import java.nio.ByteBuffer;

/**
 * ConsumeQueue物理结构对应的抽象
 */
@Data
public class ConsumeQueue {
    private int msgIndex;
    private int msgLen;
    private String commitLogName;


    public byte[] convert2Bytes() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + commitLogName.length());
        buffer.putInt(msgIndex);
        buffer.putInt(msgLen);
        buffer.put(commitLogName.getBytes());
        return buffer.array();
    }
}
