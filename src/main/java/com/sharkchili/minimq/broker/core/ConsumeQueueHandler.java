package com.sharkchili.minimq.broker.core;

import cn.hutool.core.util.StrUtil;
import com.sharkchili.minimq.broker.cache.CommitLogMappedFileCache;
import com.sharkchili.minimq.broker.cache.ConsumeQueueMappedFileCache;
import com.sharkchili.minimq.broker.cache.ConsumerGroupOffsetCache;
import com.sharkchili.minimq.broker.cache.TopicJSONCache;
import com.sharkchili.minimq.broker.entity.ConsumeQueue;
import com.sharkchili.minimq.broker.entity.ConsumeQueueOffset;
import com.sharkchili.minimq.broker.entity.ConsumerGroupOffset;
import com.sharkchili.minimq.broker.entity.Queue;
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
    @Autowired
    private ConsumeQueueMappedFileCache consumeQueueMappedFileCache;


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

            List<ConsumeQueueOffset> consumerOffsetList = topicJSONCache.getTopic(topicName).getQueueList().stream()
                    .map(q -> {
                        ConsumeQueueOffset consumerOffset = new ConsumeQueueOffset();
                        consumerOffset.setQueueId(q.getId());
                        consumerOffset.setCommitLogName(q.getFileName());
                        consumerOffset.setMsgId(0);
                        return consumerOffset;
                    })
                    .collect(Collectors.toList());

            consumerGroupOffset.setConsumeQueueOffsetList(consumerOffsetList);
            consumerGroupOffsetCache.putConsumerGroupOffset(key, consumerGroupOffset);
        }

        //拉取当前消费者组对应topic的消费进度
        consumerGroupOffset = consumerGroupOffsetCache.getConsumerGroupOffset(key);
        //定位到具体队列的值
        ConsumeQueueOffset consumerOffset = consumerGroupOffset.getConsumeQueueOffsetList().get(queueId);

        //到consumeQueue中拿到具体的offset的值
        ConsumeQueueMappedFile consumeQueueMappedFile = consumeQueueMappedFileCache.get(topicName).get(queueId);
        byte[] bytes = consumeQueueMappedFile.read(consumerOffset.getMsgId());
        ConsumeQueue consumeQueue = new ConsumeQueue();
        consumeQueue.convert2Obj(bytes);




        return null;

    }

}
