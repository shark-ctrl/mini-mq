package com.sharkchili.minimq.broker.model;

import lombok.Data;

import java.util.List;

@Data
public class Topic {

    private CommitLog commitLog;
    /**
     * topic名称
     */
    private String topicName;
    /**
     * 当前topic创建时间
     */
    private Long createTime;
    /**
     * 当前topic更新时间
     */
    private Long updateTime;
    private List<Queue> queueList;
}
