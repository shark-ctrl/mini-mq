package com.sharkchili.minimq.broker.entity;

import java.nio.ByteBuffer;

public class Message {
    //记录消息的size
    private int size;
    //消息的字符串表示形式
    private String content;

    public Message() {

    }

    public Message(String content) {
        this.content = content;
        this.size = content.getBytes().length;
    }


    public byte[] convert2Bytes() {
        //计算消息的字节大小
        ByteBuffer buffer = ByteBuffer.allocate(size + content.getBytes().length);
        //用4个字节写入消息的size
        buffer.putInt(size);
        //写入消息
        buffer.put(content.getBytes());
        //生成byte数组返回
        return buffer.array();
    }
}
