package com.sharkchili.minimq.broker.cache;

import cn.hutool.core.io.FileUtil;
import cn.hutool.extra.spring.SpringUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import com.sharkchili.minimq.broker.config.BaseConfig;
import com.sharkchili.minimq.broker.model.Topic;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;

@Component
@Slf4j
public class TopicCache {


    private List<Topic> topicList;
    private Map<String, Topic> topicMap;
    private File topicJsonFilePath;


    @PostConstruct
    public void init() {
        topicJsonFilePath = new File(SpringUtil.getBean(BaseConfig.class).getBrokerConfPath() + "conf/mq-topic.json");
        byte[] bytes = FileUtil.readBytes(topicJsonFilePath);
        JSONArray objects = JSONUtil.parseArray(new String(bytes));
        topicList = JSONUtil.toList(objects, Topic.class);
        topicMap = JSONUtil.toList(objects, Topic.class).stream().collect(Collectors.toMap(Topic::getTopicName, Function.identity()));
        log.info("topicList:{}", JSONUtil.toJsonStr(topicList));
    }


    public Topic getTopic(String topicName) {
        return topicMap.get(topicName);
    }

    public boolean containsTopic(String topicName) {
        return topicMap.containsKey(topicName);
    }


    @Scheduled(fixedRate = 15000)
    @Async("flushTopicListScheduler")
    public void flushTopicList2Disk() {
        log.info("flush topic list  to disk,topicList:{} ,write path:{}", JSONUtil.toJsonStr(topicList), topicJsonFilePath);
        FileUtil.writeUtf8String(JSONUtil.toJsonStr(topicList), topicJsonFilePath);
    }

}
