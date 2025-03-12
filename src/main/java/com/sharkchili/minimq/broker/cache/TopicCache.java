package com.sharkchili.minimq.broker.cache;

import cn.hutool.core.io.FileUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import com.sharkchili.minimq.broker.model.Topic;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class TopicCache {


    private Map<String, Topic> topicMap;

    @PostConstruct
    public void init() throws FileNotFoundException {
        File file = ResourceUtils.getFile("classpath:conf/mq-topic.json");
        byte[] bytes = FileUtil.readBytes(file);
        JSONArray objects = JSONUtil.parseArray(new String(bytes));
        topicMap = JSONUtil.toList(objects, Topic.class).stream().collect(Collectors.toMap(Topic::getTopicName, Function.identity()));
    }


    public Topic getTopic(String topicName) {
        return topicMap.get(topicName);
    }

    public boolean containsTopic(String topicName) {
        return topicMap.containsKey(topicName);
    }

}
