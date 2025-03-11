package com.sharkchili.minimq.minimq;

import cn.hutool.core.io.FileUtil;
import com.sharkchili.minimq.broker.core.CommitLogHandler;
import com.sharkchili.minimq.broker.core.MappedFileCache;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
@Slf4j
class MiniMqApplicationTests {

    @Autowired
    private CommitLogHandler commitLogHandler;


    @Test
    void contextLoads() {
    }


    @Test
    public void testMmapAppend() throws IOException {
        FileUtil.writeBytes("".getBytes(), "F:\\tmp\\0000000");
        commitLogHandler.loadCommitLogFile("test-topic", "F:\\tmp\\0000000");
        commitLogHandler.appendCommitLog("test-topic", "this is a test message");

        byte[] bytes = commitLogHandler.readCommitLog("test-topic");
        String s = new String(bytes, "utf-8");
        log.info("the commit log message is :{}", s);

        commitLogHandler.cleanCommitLog("test-topic");

    }

}
