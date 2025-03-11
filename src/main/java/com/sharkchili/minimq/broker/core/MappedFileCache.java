package com.sharkchili.minimq.broker.core;

import com.sharkchili.minimq.broker.MappedFile;
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
}
