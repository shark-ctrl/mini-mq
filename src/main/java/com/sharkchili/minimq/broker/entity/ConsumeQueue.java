package com.sharkchili.minimq.broker.entity;

import lombok.Data;

@Data
public class ConsumeQueue {
    private String commitLogName;
    private int msgIndex;
    private int msgLen;
}
