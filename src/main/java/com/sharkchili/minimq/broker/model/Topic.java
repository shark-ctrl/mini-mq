package com.sharkchili.minimq.broker.model;

import lombok.Data;

import java.util.List;

@Data
public class Topic {
    private CommitLog commitLog;
    private String topicName;
    private Long createTime;
    private Long updateTime;
    private List<Queue> queueList;
}
