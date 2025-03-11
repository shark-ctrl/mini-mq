package com.sharkchili.minimq.broker.core;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.rmi.RemoteException;

@Component
@Slf4j
public class CommitLogHandler {

    @Autowired
    private MappedFileCache mappedFileCache;

    public void loadCommitLogFile(String topicName, String fileName) throws IOException {

        if (StrUtil.isEmpty(fileName) ||
                StrUtil.isEmpty(topicName)) {
            throw new RuntimeException("invalid topicName or fileName is null");
        }

        MappedFile mappedFile = new MappedFile(fileName, 0, 1 * 1024 * 1024);
        mappedFileCache.put(topicName, mappedFile);
    }

    public void appendCommitLog(String topicName, String msg) throws IOException {
        if (StrUtil.isEmpty(msg)) {
            throw new RuntimeException("invalid msg is null");
        }

        if (!mappedFileCache.containsTopic(topicName)) {
            throw new RuntimeException("topic not exist");
        }


        MappedFile mappedFile = mappedFileCache.get(topicName);
        mappedFile.write(msg.getBytes());
    }


    public byte[] readCommitLog(String topicName) throws IOException {
        if (StrUtil.isEmpty(topicName)) {
            throw new RuntimeException("invalid topicName is null");
        }
        if (!mappedFileCache.containsTopic(topicName)) {
            throw new RuntimeException("topic not exist");
        }

        MappedFile mappedFile = mappedFileCache.get(topicName);
        return mappedFile.read(0, 1 * 1024 * 1024);
    }

}
