package com.sharkchili.minimq.broker.entity;

import lombok.Data;

@Data
public class Queue {
    private Integer id;
    private Long minOffset;
    private Long maxOffset;
    private Long currentOffset;
}
