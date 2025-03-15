package com.sharkchili.minimq.broker.core;

import cn.hutool.core.util.StrUtil;
import com.sharkchili.minimq.broker.cache.ConsumerGroupOffsetCache;
import com.sharkchili.minimq.broker.cache.TopicJSONCache;
import com.sharkchili.minimq.broker.entity.ConsumerGroupOffset;
import com.sharkchili.minimq.broker.entity.ConsumerOffset;
import com.sharkchili.minimq.broker.entity.Topic;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class ConsumeQueueHandler {

    @Autowired
    private ConsumerGroupOffsetCache consumerGroupOffsetCache;
    @Autowired
    private TopicJSONCache topicJSONCache;


    public byte[] pullMessages(String topicName, String consumeGroup, Integer queueId) {
        if (StrUtil.isEmpty(topicName)) {
            throw new IllegalArgumentException("topicName is null");
        }
        if (StrUtil.isEmpty(consumeGroup)) {
            throw new IllegalArgumentException("consumeGroup is null");
        }

        if (!topicJSONCache.containsTopic(topicName)) {
            throw new IllegalArgumentException("topic is not exist");
        }

        String key = topicName + "-" + consumeGroup;
        ConsumerGroupOffset consumerGroupOffset;

        if (!consumerGroupOffsetCache.contaninConsumerGroupOffset(key)) {
            consumerGroupOffset = new ConsumerGroupOffset();
            consumerGroupOffset.setConsumerGroupName(consumeGroup);
            consumerGroupOffset.setTopicName(topicName);

            List<ConsumerOffset> consumerOffsetList = topicJSONCache.getTopic(topicName).getQueueList().stream()
                    .map(q -> {
                        ConsumerOffset consumerOffset = new ConsumerOffset();
                        consumerOffset.setQueueId(q.getId());
                        consumerOffset.setCommitLogName(q.getFileName());
                        consumerOffset.setMsgId(0);
                        return consumerOffset;
                    })
                    .collect(Collectors.toList());

            consumerGroupOffset.setConsumerOffsetList(consumerOffsetList);
            consumerGroupOffsetCache.putConsumerGroupOffset(key, consumerGroupOffset);

        }


         consumerGroupOffset = consumerGroupOffsetCache.getConsumerGroupOffset(key);

        return null;

    }

}
