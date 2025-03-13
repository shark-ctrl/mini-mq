package com.sharkchili.minimq.broker.core;

import cn.hutool.core.util.StrUtil;
import com.sharkchili.minimq.broker.cache.CommitLogMappedFileCache;
import com.sharkchili.minimq.broker.entity.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static com.sharkchili.minimq.broker.constants.BrokerConstants.COMMIT_LOG_DEFAULT_MMAP_SIZE;

@Component
@Slf4j
public class CommitLogHandler {

    @Autowired
    private CommitLogMappedFileCache commitLogMappedFileCache;

    public void loadCommitLogFile(String topicName, String fileName) throws Exception {

        if (StrUtil.isEmpty(fileName) ||
                StrUtil.isEmpty(topicName)) {
            throw new RuntimeException("invalid topicName or fileName is null");
        }

        CommitLogMappedFile mappedFile = new CommitLogMappedFile(fileName);
        mappedFile.loadFileWithMmap(topicName, 0, 1 * 1024 * 1024);
        commitLogMappedFileCache.put(topicName, mappedFile);
    }

    public void appendMsgCommitLog(String topicName, String msg) throws Exception {
        if (StrUtil.isEmpty(msg)) {
            throw new RuntimeException("invalid msg is null");
        }

        if (!commitLogMappedFileCache.containsTopic(topicName)) {
            throw new RuntimeException("topic not exist");
        }


        CommitLogMappedFile mappedFile = commitLogMappedFileCache.get(topicName);
        mappedFile.write(new Message(msg));
    }


    public byte[] readCommitLog(String topicName) throws IOException {
        if (StrUtil.isEmpty(topicName)) {
            throw new RuntimeException("invalid topicName is null");
        }
        if (!commitLogMappedFileCache.containsTopic(topicName)) {
            throw new RuntimeException("topic not exist");
        }

        CommitLogMappedFile mappedFile = commitLogMappedFileCache.get(topicName);
        return mappedFile.read(0, 20);
    }


    public void cleanCommitLog(String topicName) throws IOException {
        if (StrUtil.isEmpty(topicName)) {
            throw new RuntimeException("invalid topicName is null");
        }
        if (!commitLogMappedFileCache.containsTopic(topicName)) {
            throw new RuntimeException("topic not exist");
        }
        CommitLogMappedFile mappedFile = commitLogMappedFileCache.get(topicName);
        mappedFile.cleaner();
    }

}
