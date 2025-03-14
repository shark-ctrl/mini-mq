package com.sharkchili.minimq.broker.cache;

import cn.hutool.core.io.FileUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import com.sharkchili.minimq.broker.config.BaseConfig;
import com.sharkchili.minimq.broker.entity.ConsumerGroupOffset;
import com.sharkchili.minimq.broker.entity.Topic;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@Slf4j
public class ConsumerGroupOffsetCache {

    @Autowired
    private BaseConfig baseConfig;

    private Map<String, ConsumerGroupOffset> consumerGroupOffsetMap;

    private String filePath;

    @PostConstruct
    public void init() {
        filePath = baseConfig.getBrokerConfPath() + File.separator + "consumequeue-offset.json";
        byte[] bytes = FileUtil.readBytes(filePath);
        JSONArray jsonArray = JSONUtil.parseArray(bytes);
        List<ConsumerGroupOffset> consumerGroupOffsetList = JSONUtil.toList(jsonArray, ConsumerGroupOffset.class);
        consumerGroupOffsetMap = consumerGroupOffsetList.stream()
                .collect(Collectors.toMap(c -> c.getTopicName() + "-" + c.getConsumerGroupName(), Function.identity()));
    }


    @Scheduled(fixedRate = 10_000)
    @Async("flush2DiskScheduler")
    public void flushTopicList2Disk() {
        List<ConsumerGroupOffset> list = new ArrayList<>(consumerGroupOffsetMap.values());
        FileUtil.writeUtf8String(JSONUtil.toJsonStr(list), filePath);

    }

}
