package com.sharkchili.minimq.broker.entity;

import cn.hutool.core.util.NumberUtil;
import lombok.Data;

import java.nio.ByteBuffer;

/**
 * ConsumeQueue物理结构对应的抽象
 */
@Data
public class ConsumeQueue {
    private int msgIndex;
    private int msgLen;
    private int  commitLogNo;


    public byte[] convert2Bytes() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 +4);
        buffer.putInt(msgIndex);
        buffer.putInt(msgLen);
        buffer.putInt(commitLogNo);
        return buffer.array();
    }

    public void convert2Obj(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        msgIndex = buffer.getInt();
        msgLen = buffer.getInt();
        commitLogNo = buffer.getInt();

    }
}
