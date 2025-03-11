package com.sharkchili.minimq.broker.config;

import cn.hutool.core.io.FileUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import com.sharkchili.minimq.broker.model.Topic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

@Component
public class TopicConfig {

    @Autowired
    private ResourceLoader resourceLoader;

    private List<Topic> topicList;

    @PostConstruct
    public void init() throws FileNotFoundException {
        File file = ResourceUtils.getFile("classpath:conf/mq-topic.json");
        byte[] bytes = FileUtil.readBytes(file);
        JSONArray objects = JSONUtil.parseArray(new String(bytes));
        topicList = JSONUtil.toList(objects, Topic.class);
    }

}
