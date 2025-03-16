package com.sharkchili.minimq.broker.cache;

import com.sharkchili.minimq.broker.config.BaseConfig;
import com.sharkchili.minimq.broker.core.CommitLogMappedFile;
import com.sharkchili.minimq.broker.core.ConsumeQueueMappedFile;
import com.sharkchili.minimq.broker.entity.Queue;
import com.sharkchili.minimq.broker.entity.Topic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.sharkchili.minimq.broker.constants.BrokerConstants.COMMIT_LOG_DEFAULT_MMAP_SIZE;

@Component
public class ConsumeQueueMappedFileCache {

    private Map<String, List<ConsumeQueueMappedFile>> cache = new ConcurrentHashMap<>();

    @Autowired
    private TopicJSONCache topicJSONCache;

    @Autowired
    private BaseConfig baseConfig;

    @PostConstruct
    public void init() {
        for (Map.Entry<String, Topic> e : topicJSONCache.getTopMap().entrySet()) {
            String topicName = e.getKey();
            List<Queue> queueList = e.getValue().getQueueList();
            List<ConsumeQueueMappedFile> consumeQueueMappedFileList = queueList.stream()
                    .map(q -> {
                        String path = baseConfig.getConsumeQueuePath() +
                                File.separator +
                                topicName +
                                File.separator +
                                q.getFileName();
                        ConsumeQueueMappedFile consumeQueueMappedFile = new ConsumeQueueMappedFile(new File(path), topicName, q.getId());
                        return consumeQueueMappedFile;
                    })
                    .peek(c -> c.loadFileWithMmap(topicName, 0, COMMIT_LOG_DEFAULT_MMAP_SIZE))
                    .collect(Collectors.toList());

            cache.put(topicName, consumeQueueMappedFileList);
        }

    }

    public void put(String topicName, List<ConsumeQueueMappedFile> file) {
        cache.put(topicName, file);
    }

    public List<ConsumeQueueMappedFile> get(String topicName) {
        return cache.get(topicName);
    }


    public boolean containsConsumeQueue(String key) {
        return cache.containsKey(key);
    }
}
