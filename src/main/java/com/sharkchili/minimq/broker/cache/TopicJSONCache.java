package com.sharkchili.minimq.broker.cache;

import cn.hutool.core.io.FileUtil;
import cn.hutool.extra.spring.SpringUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import com.sharkchili.minimq.broker.config.BaseConfig;
import com.sharkchili.minimq.broker.entity.Topic;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@Slf4j
public class TopicJSONCache {


    private Map<String, Topic> topicMap;
    private File topicJsonFilePath;


    @PostConstruct
    public void init() {
        //获取文件路径
        topicJsonFilePath = new File(SpringUtil.getBean(BaseConfig.class).getBrokerConfPath() + "conf/mq-topic.json");
        //读取文件并反序列化
        byte[] bytes = FileUtil.readBytes(topicJsonFilePath);
        JSONArray objects = JSONUtil.parseArray(new String(bytes));
        //以topicName作为key，topic作为value生成map
        topicMap = JSONUtil.toList(objects, Topic.class).stream().collect(Collectors.toMap(Topic::getTopicName, Function.identity()));
        log.info("topicList:{}", JSONUtil.toJsonStr(topicMap));
    }


    public Topic getTopic(String topicName) {
        return topicMap.get(topicName);
    }

    public Map<String, Topic> getTopMap() {
        return topicMap;
    }

    public boolean containsTopic(String topicName) {
        return topicMap.containsKey(topicName);
    }


    @Scheduled(fixedRate = 10_000)
    @Async("flushTopicListScheduler")
    public void flushTopicList2Disk() {
        log.info("flush topic list  to disk,topicList:{} ,write path:{}", JSONUtil.toJsonStr(topicMap), topicJsonFilePath);
        ArrayList<Topic> topicArrayList = new ArrayList<>(topicMap.values());
        FileUtil.writeUtf8String(JSONUtil.toJsonStr(topicArrayList), topicJsonFilePath);
    }

}
