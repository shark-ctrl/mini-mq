package com.sharkchili.minimq.broker.entity;

import lombok.Data;

@Data
public class ConsumerOffset {
    private int queueId;
    private String commitLogName;
    private int msgId;
}
