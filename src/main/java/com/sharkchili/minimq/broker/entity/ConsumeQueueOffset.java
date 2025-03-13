package com.sharkchili.minimq.broker.entity;

import lombok.Data;

import java.util.List;

@Data
public class ConsumeQueueOffset {
    private String topicName;
    private String consumeGroup;
    private List<ConsumeOffset> consumeOffsetList;


    @Data
    private class ConsumeOffset {
        private String queueId;
        private String commitLogName;
        private int msgId;
    }
}
