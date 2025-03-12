package com.sharkchili.minimq.broker.core;

import cn.hutool.core.util.StrUtil;
import com.sharkchili.minimq.broker.cache.MappedFileCache;
import com.sharkchili.minimq.broker.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class CommitLogHandler {

    @Autowired
    private MappedFileCache mappedFileCache;

    public void loadCommitLogFile(String topicName, String fileName) throws Exception {

        if (StrUtil.isEmpty(fileName) ||
                StrUtil.isEmpty(topicName)) {
            throw new RuntimeException("invalid topicName or fileName is null");
        }

        MappedFile mappedFile = new MappedFile(fileName);
        mappedFile.loadFileWithMmap(topicName, 0, 1 * 1024 * 1024);
        mappedFileCache.put(topicName, mappedFile);
    }

    public void appendMsgCommitLog(String topicName, String msg) throws Exception {
        if (StrUtil.isEmpty(msg)) {
            throw new RuntimeException("invalid msg is null");
        }

        if (!mappedFileCache.containsTopic(topicName)) {
            throw new RuntimeException("topic not exist");
        }


        MappedFile mappedFile = mappedFileCache.get(topicName);
        mappedFile.write(new Message(msg));
    }


    public byte[] readCommitLog(String topicName) throws IOException {
        if (StrUtil.isEmpty(topicName)) {
            throw new RuntimeException("invalid topicName is null");
        }
        if (!mappedFileCache.containsTopic(topicName)) {
            throw new RuntimeException("topic not exist");
        }

        MappedFile mappedFile = mappedFileCache.get(topicName);
        return mappedFile.read(0, 14);
    }


    public void cleanCommitLog(String topicName) throws IOException {
        if (StrUtil.isEmpty(topicName)) {
            throw new RuntimeException("invalid topicName is null");
        }
        if (!mappedFileCache.containsTopic(topicName)) {
            throw new RuntimeException("topic not exist");
        }
        MappedFile mappedFile = mappedFileCache.get(topicName);
        mappedFile.cleaner();
    }

}
