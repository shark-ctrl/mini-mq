package com.sharkchili.minimq.broker.cache;

import cn.hutool.core.io.FileUtil;
import cn.hutool.extra.spring.SpringUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import com.sharkchili.minimq.broker.config.BaseConfig;
import com.sharkchili.minimq.broker.entity.ConsumeQueueOffset;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.List;


@Component
@Slf4j
@Data
public class ConsumeQueueOffsetCache {


    @Autowired
    private BaseConfig baseConfig;

    private File consumeOffsetJsonFile;

    private List<ConsumeQueueOffset> consumeQueueOffsetList;



    @PostConstruct
    public void init() {

        consumeOffsetJsonFile = new File(SpringUtil.getBean(BaseConfig.class).getBrokerConfPath() + "conf/consumequeue-offset.json");
        byte[] bytes = FileUtil.readBytes(consumeOffsetJsonFile);
        JSONArray objects = JSONUtil.parseArray(new String(bytes));
        consumeQueueOffsetList = JSONUtil.toList(objects, ConsumeQueueOffset.class);
        log.info("topicList:{}", JSONUtil.toJsonStr(consumeQueueOffsetList));
    }



    @Scheduled(fixedRate = 15000)
    @Async("flushTopicListScheduler")
    public void flushTopicList2Disk() {
        log.info("flush topic consumeQueueOffsetList  to disk,consumeQueueOffsetList:{} ,write path:{}", JSONUtil.toJsonStr(consumeQueueOffsetList), consumeOffsetJsonFile);
        FileUtil.writeUtf8String(JSONUtil.toJsonStr(consumeQueueOffsetList), consumeOffsetJsonFile);
    }

}
