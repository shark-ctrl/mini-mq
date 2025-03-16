package com.sharkchili.minimq.broker.entity;

import lombok.Data;

import java.util.List;

@Data
public class ConsumerGroupOffset {
    private String topicName;
    private String consumerGroupName;
    private List<ConsumeQueueOffset> consumeQueueOffsetList;


}
