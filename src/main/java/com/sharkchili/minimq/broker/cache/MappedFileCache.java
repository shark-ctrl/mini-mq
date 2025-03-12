package com.sharkchili.minimq.broker.cache;

import com.sharkchili.minimq.broker.core.MappedFile;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class MappedFileCache {

    private Map<String, MappedFile> cache = new ConcurrentHashMap<>();


    public void put(String key, MappedFile file) {
        cache.put(key, file);
    }

    public MappedFile get(String key) {
        return cache.get(key);
    }


    public boolean containsTopic(String topicName) {
        return cache.containsKey(topicName);
    }
}
