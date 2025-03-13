package com.sharkchili.minimq.broker.entity;

import lombok.Data;

@Data
public class Queue {
    private Integer id;
    private int minOffset;
    private int maxOffset;
    private int currentOffset;
    private String fileName;
}
