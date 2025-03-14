package com.sharkchili.minimq.broker.entity;

import lombok.Data;

/**
 * ConsumeQueue物理结构对应的抽象
 */
@Data
public class ConsumeQueue {
    private String commitLogName;
    private int msgIndex;
    private int msgLen;
}
