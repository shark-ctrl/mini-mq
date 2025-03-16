package com.sharkchili.minimq.broker.cache;

import com.sharkchili.minimq.broker.core.CommitLogMappedFile;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class CommitLogMappedFileCache {

    private Map<String, CommitLogMappedFile> cache = new ConcurrentHashMap<>();


    public void put(String key, CommitLogMappedFile file) {
        cache.put(key, file);
    }

    public CommitLogMappedFile get(String topicName) {
        return cache.get(topicName);
    }


    public boolean containsTopic(String topicName) {
        return cache.containsKey(topicName);
    }
}
