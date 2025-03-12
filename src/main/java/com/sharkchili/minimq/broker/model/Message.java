package com.sharkchili.minimq.broker.model;

import java.nio.ByteBuffer;

public class Message {
    private int size;
    private String content;

    public Message() {

    }

    public Message(String content) {
        this.content = content;
        this.size = content.getBytes().length;
    }


    public byte[] convert2Bytes() {
        ByteBuffer buffer = ByteBuffer.allocate(size + content.getBytes().length);
        buffer.putInt(size);
        buffer.put(content.getBytes());
        return buffer.array();
    }
}
