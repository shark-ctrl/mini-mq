package com.sharkchili.minimq.broker.model;

import lombok.Data;

import java.util.List;

@Data
public class Topic {
    private String topicName;
    private List<Queue> queueList;
    private Long createTime;
    private Long updateTime;
    private CommitLog commitLog;
}
