package com.sharkchili.minimq.broker.entity;

import lombok.Data;

@Data
public class ConsumeQueueOffset {
    private int queueId;
    private String commitLogName;
    private int msgId;
}
